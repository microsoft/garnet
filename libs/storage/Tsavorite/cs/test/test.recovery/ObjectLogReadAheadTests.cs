// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using NUnit.Framework;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.recovery
{
    using ClassAllocator = ObjectAllocator<StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>>;
    using ClassStoreFunctions = StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>;

    /// <summary>
    /// Recovery and scans read the objects of a main-log page's records through a read-ahead, so a page of small objects costs a read or
    /// two of the object log rather than one per record. Small object-log pages and memory keep most of the object log on the device.
    /// </summary>
    [TestFixture]
    internal class ObjectLogReadAheadTests : TestBase
    {
        const int NumRecords = 10_000;

        [SetUp]
        public void Setup() => RecreateDirectory(MethodTestDir);

        [TearDown]
        public void TearDown() => OnTearDown();

        /// <summary>Opens checkpoint devices as the default factory does, but counts the reads of a Snapshot's object-log file.</summary>
        sealed class CountingSnapshotObjectLogFactoryCreator : INamedDeviceFactoryCreator
        {
            readonly LocalStorageNamedDeviceFactoryCreator inner = new();
            internal readonly List<ControlledReadFailureDevice> SnapshotObjectLogDevices = [];

            public INamedDeviceFactory Create(string baseName) => new Factory(this, inner.Create(baseName));

            internal int ReadCount
            {
                get
                {
                    lock (this)
                    {
                        var count = 0;
                        foreach (var device in SnapshotObjectLogDevices)
                            count += device.ReadCount;
                        return count;
                    }
                }
            }

            sealed class Factory(CountingSnapshotObjectLogFactoryCreator owner, INamedDeviceFactory inner) : INamedDeviceFactory
            {
                public IDevice Get(FileDescriptor fileInfo)
                {
                    var device = inner.Get(fileInfo);
                    if (fileInfo.fileName != "snapshot.obj.dat")
                        return device;
                    var counting = new ControlledReadFailureDevice(device);
                    lock (owner)
                        owner.SnapshotObjectLogDevices.Add(counting);
                    return counting;
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
                ObjectLogPageSize = 1L << 12,
                ObjectLogMemorySize = 1L << 14,
                ObjectLogSegmentSize = 1L << 20,
                CheckpointManager = checkpointManager,
            }, StoreFunctions.Create(new TestObjectKey.Comparer(), () => new TestObjectValue.Serializer()),
               (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));

        static DeviceLogCommitCheckpointManager CreateCheckpointManager(INamedDeviceFactoryCreator deviceFactoryCreator = null)
            => new(deviceFactoryCreator ?? new LocalStorageNamedDeviceFactoryCreator(),
                new DefaultCheckpointNamingScheme(new DirectoryInfo(Path.Join(MethodTestDir, "checkpoints")).FullName));

        static IDevice CreateLogDevice() => Devices.CreateLogDevice(Path.Join(MethodTestDir, "hlog.log"), deleteOnClose: false);

        static IDevice CreateObjectLogDevice() => Devices.CreateLogDevice(Path.Join(MethodTestDir, "hlog.obj.log"), deleteOnClose: false);

        static ClientSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions, ClassStoreFunctions, ClassAllocator> NewSession(
                TsavoriteKV<ClassStoreFunctions, ClassAllocator> store)
            => store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions());

        static void Populate(TsavoriteKV<ClassStoreFunctions, ClassAllocator> store)
        {
            using var session = NewSession(store);
            for (var key = 0; key < NumRecords; key++)
                _ = session.BasicContext.Upsert(new TestObjectKey { key = key }, new TestObjectValue { value = key });
        }

        static int PagesSpanned(TsavoriteKV<ClassStoreFunctions, ClassAllocator> store)
            => store.hlogBase.GetPage(store.Log.TailAddress) - store.hlogBase.GetPage(store.Log.BeginAddress) + 1;

        static void VerifyAll(TsavoriteKV<ClassStoreFunctions, ClassAllocator> store)
        {
            using var session = NewSession(store);
            var context = session.BasicContext;
            for (var key = 0; key < NumRecords; key++)
            {
                TestObjectInput input = default;
                TestObjectOutput output = new();
                var status = context.Read(new TestObjectKey { key = key }, ref input, ref output);
                if (status.IsPending)
                {
                    Assert.That(context.CompletePendingWithOutputs(out var completedOutputs, wait: true), Is.True);
                    (status, output) = GetSinglePendingResult(completedOutputs);
                }
                Assert.That(status.Found, Is.True, $"key {key} not found");
                Assert.That(output.value?.value, Is.EqualTo(key), $"key {key} has the wrong value");
            }
        }

        /// <summary>
        /// Recovery loads each recovered page's objects, and a Snapshot's recovery also copies its snapshot-region objects into the main
        /// object log; with the read-ahead, each walk of a main-log page of small objects takes a read, or two where its objects cross an
        /// object-log page, rather than one per record.
        /// </summary>
        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task RecoveryReadsSmallObjectsInAboutAReadPerPage([Values(CheckpointType.FoldOver, CheckpointType.Snapshot)] CheckpointType checkpointType)
        {
            Guid token;
            int pagesSpanned;
            using (var log = CreateLogDevice())
            using (var objlog = CreateObjectLogDevice())
            using (var checkpointManager = CreateCheckpointManager())
            {
                var store = CreateStore(log, objlog, checkpointManager);
                try
                {
                    Populate(store);
                    pagesSpanned = PagesSpanned(store);
                    Assert.That(store.TryInitiateHybridLogCheckpoint(out token, checkpointType), Is.True);
                    await store.CompleteCheckpointAsync().AsTask().ConfigureAwait(false);
                }
                finally
                {
                    store.Dispose();
                }
            }

            using var recoveryLog = CreateLogDevice();
            using var objectLogFile = CreateObjectLogDevice();
            var countingObjectLog = new ControlledReadFailureDevice(objectLogFile);
            var countingFactory = new CountingSnapshotObjectLogFactoryCreator();
            using var recoveryCheckpointManager = CreateCheckpointManager(countingFactory);
            var recoverStore = CreateStore(recoveryLog, countingObjectLog, recoveryCheckpointManager);
            try
            {
                _ = await recoverStore.RecoverAsync(default, token).ConfigureAwait(false);
                var reads = countingObjectLog.ReadCount + countingFactory.ReadCount;

                // A Snapshot's recovery walks each page twice, to load its objects and to copy them.
                var walks = checkpointType == CheckpointType.Snapshot ? 2 : 1;
                Assert.That(reads, Is.LessThanOrEqualTo(walks * 2 * pagesSpanned),
                    $"{checkpointType} recovery of {NumRecords} small objects on {pagesSpanned} pages took {reads} object-log reads");
                VerifyAll(recoverStore);
            }
            finally
            {
                recoverStore.Dispose();
            }
        }

        /// <summary>A scan of pages on disk loads each page's objects with a read or two of the object log, rather than one per record.</summary>
        [Test]
        [Category("TsavoriteKV")]
        public void ScanReadsSmallObjectsInAboutAReadPerPage()
        {
            using var log = CreateLogDevice();
            using var objectLogFile = CreateObjectLogDevice();
            var countingObjectLog = new ControlledReadFailureDevice(objectLogFile);
            using var checkpointManager = CreateCheckpointManager();
            var store = CreateStore(log, countingObjectLog, checkpointManager);
            try
            {
                Populate(store);
                var pagesSpanned = PagesSpanned(store);
                store.Log.FlushAndEvict(wait: true);

                var readsBefore = countingObjectLog.ReadCount;
                var scanned = 0;
                using (var iter = store.Log.Scan(store.Log.BeginAddress, store.Log.TailAddress))
                {
                    while (iter.GetNext())
                    {
                        Assert.That(((TestObjectValue)iter.ValueObject).value, Is.EqualTo(iter.Key.AsRef<TestObjectKey>().key), "scanned value");
                        scanned++;
                    }
                }
                var reads = countingObjectLog.ReadCount - readsBefore;

                Assert.That(scanned, Is.EqualTo(NumRecords), "records scanned");
                Assert.That(reads, Is.LessThanOrEqualTo(2 * pagesSpanned),
                    $"a scan of {NumRecords} small objects on {pagesSpanned} pages took {reads} object-log reads");
            }
            finally
            {
                store.Dispose();
            }
        }
    }
}