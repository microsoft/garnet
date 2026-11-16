// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.recovery
{
    using ClassAllocator = ObjectAllocator<StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>>;
    using ClassStoreFunctions = StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>;

    /// <summary>
    /// A recovery that fails or is cancelled must still close the checkpoint files it opened, and must not return while reads or flushes
    /// it issued are in flight: cancelling a wait does not cancel the IO it was waiting for, which keeps reading the checkpoint file and
    /// writing into log pages the caller may free once recovery throws.
    /// </summary>
    [TestFixture]
    public class FailedRecoveryCleanupTests : TestBase
    {
        const int NumRecords = 2000;

        [SetUp]
        public void Setup() => RecreateDirectory(MethodTestDir);

        [TearDown]
        public void TearDown() => OnTearDown();

        /// <summary>Opens checkpoint devices as the default factory does, but returns the Snapshot's main-log and object-log files wrapped, so a
        /// test can hold a read on the first and see whether each was closed.</summary>
        sealed class SnapshotDeviceInterceptingFactoryCreator : INamedDeviceFactoryCreator
        {
            readonly LocalStorageNamedDeviceFactoryCreator inner = new();
            internal readonly List<DeferredCompletionDevice> SnapshotLogDevices = [];
            internal readonly List<DeferredCompletionDevice> SnapshotObjectLogDevices = [];

            public INamedDeviceFactory Create(string baseName) => new Factory(this, inner.Create(baseName));

            sealed class Factory(SnapshotDeviceInterceptingFactoryCreator owner, INamedDeviceFactory inner) : INamedDeviceFactory
            {
                public IDevice Get(FileDescriptor fileInfo)
                {
                    var device = inner.Get(fileInfo);
                    if (fileInfo.fileName == "snapshot.dat")
                    {
                        var wrapped = new DeferredCompletionDevice(device);
                        lock (owner)
                            owner.SnapshotLogDevices.Add(wrapped);
                        return wrapped;
                    }
                    if (fileInfo.fileName == "snapshot.obj.dat")
                    {
                        var wrapped = new DeferredCompletionDevice(device) { DeferReadNumber = int.MaxValue };
                        lock (owner)
                            owner.SnapshotObjectLogDevices.Add(wrapped);
                        return wrapped;
                    }
                    return device;
                }

                public void Delete(FileDescriptor fileInfo) => inner.Delete(fileInfo);

                public IEnumerable<FileDescriptor> ListContents(string path) => inner.ListContents(path);
            }
        }

        static TsavoriteKV<ClassStoreFunctions, ClassAllocator> CreateStore(IDevice log, IDevice objlog, ICheckpointManager checkpointManager)
            => new(new()
            {
                IndexSize = 1L << 16,
                LogDevice = log,
                ObjectLogDevice = objlog,
                SegmentSize = 1L << 20,
                LogMemorySize = 1L << 20,
                PageSize = MinKvLogPageSize,
                CheckpointManager = checkpointManager,
            }, StoreFunctions.Create(new TestObjectKey.Comparer(), () => new TestObjectValue.Serializer()),
               (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));

        static DeviceLogCommitCheckpointManager CreateCheckpointManager(INamedDeviceFactoryCreator deviceFactoryCreator = null)
            => new(deviceFactoryCreator ?? new LocalStorageNamedDeviceFactoryCreator(),
                new DefaultCheckpointNamingScheme(new DirectoryInfo(Path.Join(MethodTestDir, "checkpoints")).FullName));

        static IDevice CreateLogDevice() => Devices.CreateLogDevice(Path.Join(MethodTestDir, "hlog.log"), deleteOnClose: false);

        static IDevice CreateObjectLogDevice() => Devices.CreateLogDevice(Path.Join(MethodTestDir, "hlog.obj.log"), deleteOnClose: false);

        /// <summary>Populate a store and take a checkpoint of <paramref name="checkpointType"/>. Every record stays in memory, so a Snapshot puts
        /// them all in its snapshot file, and FoldOver flushes them all to the main log.</summary>
        static async Task<Guid> BuildCheckpointAsync(CheckpointType checkpointType)
        {
            using var log = CreateLogDevice();
            using var objlog = CreateObjectLogDevice();
            using var checkpointManager = CreateCheckpointManager();
            using var store = CreateStore(log, objlog, checkpointManager);
            using (var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions()))
            {
                var bContext = session.BasicContext;
                for (var key = 0; key < NumRecords; key++)
                    _ = bContext.Upsert(new TestObjectKey { key = key }, new TestObjectValue { value = key });
            }

            ClassicAssert.IsTrue(store.TryInitiateHybridLogCheckpoint(out var token, checkpointType), "failed to initiate the checkpoint");
            await store.CompleteCheckpointAsync().ConfigureAwait(false);
            return token;
        }

        /// <summary>Cancel a recovery while a read of <paramref name="heldDevice"/> is held, and verify that the recovery returns only once that read
        /// has called back.</summary>
        static void CancelWhileAReadIsHeld(TsavoriteKV<ClassStoreFunctions, ClassAllocator> store, Guid token, DeferredCompletionDevice heldDevice,
            Func<DeferredCompletionDevice> getHeldDevice = null)
        {
            using var cts = new CancellationTokenSource();
            var recovery = Task.Run(async () => await store.RecoverAsync(default, token, cancellationToken: cts.Token).ConfigureAwait(false));

            // Recovery is waiting on the held read; cancelling abandons that wait, but not the read.
            var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(30);
            while ((heldDevice ??= getHeldDevice?.Invoke()) is null || heldDevice.DeferredCount == 0)
            {
                ClassicAssert.IsFalse(recovery.IsCompleted, $"Recovery finished before it read the held device: {recovery.Exception?.GetBaseException().Message}");
                ClassicAssert.Less(DateTime.UtcNow, deadline, "Recovery never issued the read to hold");
                Thread.Sleep(10);
            }
            cts.Cancel();

            var settled = recovery.ContinueWith(_ => { }, TaskScheduler.Default);
            ClassicAssert.IsFalse(settled.Wait(TimeSpan.FromMilliseconds(250)), "Recovery must not return while its read is outstanding");

            heldDevice.CompleteDeferred();
            ClassicAssert.IsTrue(settled.Wait(TimeSpan.FromSeconds(30)), "Recovery must return once the outstanding read has called back");
            ClassicAssert.IsTrue(recovery.IsCanceled || recovery.IsFaulted, "Recovery must report the cancellation");
        }

        static void VerifyAllRecords(TsavoriteKV<ClassStoreFunctions, ClassAllocator> store)
        {
            using var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions());
            var bContext = session.BasicContext;
            for (var key = 0; key < NumRecords; key++)
            {
                TestObjectInput input = default;
                TestObjectOutput output = new();
                var status = bContext.Read(new TestObjectKey { key = key }, ref input, ref output);
                if (status.IsPending)
                {
                    ClassicAssert.IsTrue(bContext.CompletePendingWithOutputs(out var completedOutputs, wait: true));
                    (status, output) = GetSinglePendingResult(completedOutputs);
                }
                ClassicAssert.IsTrue(status.Found, $"key {key} not found");
                ClassicAssert.AreEqual(key, output.value.value, $"key {key}");
            }
        }

        /// <summary>
        /// A cancelled Snapshot recovery closes the snapshot's main-log and object-log files, and closes the main-log file only after the read
        /// it was waiting on has called back. The checkpoint stays recoverable.
        /// </summary>
        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task CancelledSnapshotRecoveryClosesItsFilesOnlyAfterItsReadsHaveCompleted()
        {
            var token = await BuildCheckpointAsync(CheckpointType.Snapshot).ConfigureAwait(false);

            var intercepting = new SnapshotDeviceInterceptingFactoryCreator();
            using (var log = CreateLogDevice())
            using (var objlog = CreateObjectLogDevice())
            using (var checkpointManager = CreateCheckpointManager(intercepting))
            using (var store = CreateStore(log, objlog, checkpointManager))
            {
                CancelWhileAReadIsHeld(store, token, heldDevice: null, getHeldDevice: () =>
                {
                    lock (intercepting)
                        return intercepting.SnapshotLogDevices.Count > 0 ? intercepting.SnapshotLogDevices[0] : null;
                });

                var snapshotLog = intercepting.SnapshotLogDevices[0];
                ClassicAssert.IsTrue(snapshotLog.Disposed, "The snapshot file must be closed on the failure path");
                ClassicAssert.IsFalse(snapshotLog.DisposedWithIoOutstanding, "The snapshot file was closed while a read was still using it");
                ClassicAssert.AreEqual(1, intercepting.SnapshotObjectLogDevices.Count, "Recovery should have opened the snapshot object log");
                ClassicAssert.IsTrue(intercepting.SnapshotObjectLogDevices[0].Disposed, "The snapshot object-log file must be closed on the failure path");
            }

            using (var log = CreateLogDevice())
            using (var objlog = CreateObjectLogDevice())
            using (var checkpointManager = CreateCheckpointManager())
            using (var store = CreateStore(log, objlog, checkpointManager))
            {
                _ = await store.RecoverAsync(default, token).ConfigureAwait(false);
                VerifyAllRecords(store);
            }
        }

        /// <summary>
        /// A cancelled FoldOver recovery returns only after the read it was waiting on has called back, so a caller that disposes the store once
        /// recovery throws cannot free a page that read is still writing into.
        /// </summary>
        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task CancelledFoldOverRecoveryReturnsOnlyAfterItsReadsHaveCompleted()
        {
            var token = await BuildCheckpointAsync(CheckpointType.FoldOver).ConfigureAwait(false);

            var log = new DeferredCompletionDevice(CreateLogDevice());
            try
            {
                using var objlog = CreateObjectLogDevice();
                using var checkpointManager = CreateCheckpointManager();
                using var store = CreateStore(log, objlog, checkpointManager);
                CancelWhileAReadIsHeld(store, token, log);
            }
            finally
            {
                log.Dispose();
            }
            ClassicAssert.IsFalse(log.DisposedWithIoOutstanding, "The log was closed while a recovery read was still using it");
        }

        /// <summary>
        /// A read whose submit throws runs no callback, so its issuer retires it, and the failed recovery returns rather than waiting for it.
        /// </summary>
        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task RecoveryReadThatThrowsOnSubmitFailsRecoveryPromptly()
        {
            var token = await BuildCheckpointAsync(CheckpointType.FoldOver).ConfigureAwait(false);

            var log = new SyncThrowOnReadDevice(CreateLogDevice()) { ThrowOnReadOrdinal = 0 };
            try
            {
                using var objlog = CreateObjectLogDevice();
                using var checkpointManager = CreateCheckpointManager();
                using var store = CreateStore(log, objlog, checkpointManager);
                var recovery = Task.Run(async () => await store.RecoverAsync(default, token).ConfigureAwait(false));
                var settled = recovery.ContinueWith(_ => { }, TaskScheduler.Default);
                ClassicAssert.IsTrue(settled.Wait(TimeSpan.FromSeconds(30)), "Recovery must fail, not wait for a read that was never submitted");
                ClassicAssert.IsTrue(log.ReadFailureInjected, "The first recovery read should have thrown on submit");
                ClassicAssert.IsTrue(recovery.IsFaulted, "The submission failure must fail recovery");
            }
            finally
            {
                log.Dispose();
            }
        }

        /// <summary>
        /// A device may run a read's callback synchronously and then throw out of the submit, so the callback and the submit's failure path both
        /// try to retire that read. Retiring it twice would let the failed recovery return while an earlier read, held open here, is outstanding.
        /// </summary>
        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task RecoveryReadThatCompletesThenThrowsIsRetiredOnlyOnce()
        {
            var token = await BuildCheckpointAsync(CheckpointType.FoldOver).ConfigureAwait(false);

            var log = new CallbackThenThrowDevice(CreateLogDevice()) { DeferReadsBefore = 1 };
            try
            {
                using var objlog = CreateObjectLogDevice();
                using var checkpointManager = CreateCheckpointManager();
                using var store = CreateStore(log, objlog, checkpointManager);
                var recovery = Task.Run(async () => await store.RecoverAsync(default, token).ConfigureAwait(false));
                var settled = recovery.ContinueWith(_ => { }, TaskScheduler.Default);

                ClassicAssert.IsFalse(settled.Wait(TimeSpan.FromMilliseconds(250)), "Recovery must not return while its first read is outstanding");
                ClassicAssert.AreEqual(1, log.DeferredCount, "The first recovery read should be held open");

                log.CompleteDeferred();
                ClassicAssert.IsTrue(settled.Wait(TimeSpan.FromSeconds(30)), "Recovery must return once the held read has called back");
                ClassicAssert.IsTrue(recovery.IsFaulted, "The submission failure must fail recovery");
            }
            finally
            {
                log.Dispose();
            }
        }
    }
}