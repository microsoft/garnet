// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.recovery
{
    using ClassAllocator = ObjectAllocator<StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>>;
    using ClassStoreFunctions = StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>;
    using SbKeyAllocator = ObjectAllocator<StoreFunctions<SpanByteComparer, DefaultRecordTriggers>>;
    using SbKeyStoreFunctions = StoreFunctions<SpanByteComparer, DefaultRecordTriggers>;

    /// <summary>
    /// Recovery and scans read the objects of a main-log page's records through a read-ahead, so a page of small objects costs a read or
    /// two of the object log rather than one per record. Small object-log pages and memory keep most of the object log on the device.
    /// </summary>
    [TestFixture]
    internal class ObjectLogReadAheadTests : TestBase
    {
        const int NumRecords = 10_000;

        /// <summary>What a capturing CopyUpdate adds to a value, so the (v+1) value differs from the (v) value recovery must return.</summary>
        const int CaptureIncrement = 1_000_000;

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

        /// <summary>Settings for the length-prefixed cases: object-log pages that hold about a hundred of their extents each.</summary>
        static KVSettings CreateLengthPrefixedSettings(IDevice log, IDevice objlog, ICheckpointManager checkpointManager) => new()
        {
            IndexSize = 1L << 16,
            LogDevice = log,
            ObjectLogDevice = objlog,
            SegmentSize = 1L << 20,
            LogMemorySize = 1L << 20,
            PageSize = 1L << 14,
            ObjectLogPageSize = 1L << 16,
            ObjectLogMemorySize = 1L << 18,
            ObjectLogSegmentSize = 1L << 22,
            CheckpointManager = checkpointManager,
        };

        static TsavoriteKV<ClassStoreFunctions, ClassAllocator> CreateLargeObjectStore(IDevice log, IDevice objlog, ICheckpointManager checkpointManager)
            => new(CreateLengthPrefixedSettings(log, objlog, checkpointManager),
                StoreFunctions.Create(new TestObjectKey.Comparer(), () => new TestLargeObjectValue.Serializer()),
                (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));

        static TsavoriteKV<SbKeyStoreFunctions, SbKeyAllocator> CreateOverflowKeyStore(IDevice log, IDevice objlog, ICheckpointManager checkpointManager)
        {
            var settings = CreateLengthPrefixedSettings(log, objlog, checkpointManager);
            settings.MaxInlineKeySize = 16;
            settings.MaxInlineValueSize = 1 << 10;
            return new(settings, StoreFunctions.Create(new SpanByteComparer(), () => new TestObjectValue.Serializer(), DefaultRecordTriggers.Instance),
                (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));
        }

        /// <summary>
        /// A FoldOver recovery loads objects before it has established the main object log's tail. An object that serializes to more
        /// than 511 bytes has a length-prefixed extent; below the checkpoint's recorded tail, it is read through its page's read-ahead
        /// rather than after a read of its own prefix.
        /// </summary>
        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task FoldOverRecoveryReadsLengthPrefixedObjectsWithoutAReadEach()
        {
            const int NumObjects = 4_000;
            const int ObjectSize = 600;     // Serializes to 604 bytes, with its 4-byte size

            Guid token;
            using (var log = CreateLogDevice())
            using (var objlog = CreateObjectLogDevice())
            using (var checkpointManager = CreateCheckpointManager())
            {
                var store = CreateLargeObjectStore(log, objlog, checkpointManager);
                try
                {
                    using (var session = store.NewSession<TestObjectKey, TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions>(new TestLargeObjectFunctions()))
                    {
                        for (var key = 0; key < NumObjects; key++)
                            _ = session.BasicContext.Upsert(new TestObjectKey { key = key }, new TestLargeObjectValue(ObjectSize), Empty.Default);
                    }
                    Assert.That(store.TryInitiateHybridLogCheckpoint(out token, CheckpointType.FoldOver), Is.True);
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
            using var recoveryCheckpointManager = CreateCheckpointManager();
            var recoverStore = CreateLargeObjectStore(recoveryLog, countingObjectLog, recoveryCheckpointManager);
            try
            {
                _ = await recoverStore.RecoverAsync(default, token).ConfigureAwait(false);
                var reads = countingObjectLog.ReadCount;
                Assert.That(reads, Is.LessThan(NumObjects / 10),
                    $"FoldOver recovery of {NumObjects} objects of {ObjectSize} bytes took {reads} object-log reads");

                using var session = recoverStore.NewSession<TestObjectKey, TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions>(new TestLargeObjectFunctions());
                var context = session.BasicContext;
                TestLargeObjectInput input = new() { wantValueStyle = TestValueStyle.Object };
                var expected = new TestLargeObjectValue(ObjectSize).value;
                for (var key = 0; key < NumObjects; key++)
                {
                    TestLargeObjectOutput output = new();
                    var status = context.Read(new TestObjectKey { key = key }, ref input, ref output, Empty.Default);
                    if (status.IsPending)
                        (status, output) = context.GetSinglePendingResult();
                    Assert.That(status.Found, Is.True, $"key {key} not found");
                    Assert.That(output.valueObject.value, Is.EqualTo(expected), $"key {key} has the wrong value");
                }
            }
            finally
            {
                recoverStore.Dispose();
            }
        }

        /// <summary>
        /// Pass 1 hashes each overflow key, reading it from the object log before recovery has established the main object log's tail. A
        /// key longer than 511 bytes has a length-prefixed extent; below the checkpoint's recorded tail it takes one read, not a read of
        /// its prefix and then another of the key. Every record is flushed and evicted before the checkpoint, so a Snapshot's main-log
        /// range holds every key, and the object load after Pass 1 reads almost nothing.
        /// </summary>
        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task RecoveryReadsEachOverflowKeyOnce([Values(CheckpointType.FoldOver, CheckpointType.Snapshot)] CheckpointType checkpointType)
        {
            const int NumKeys = 2_000;
            const int KeySize = 1_100;      // Longer than Garnet's default 1,022-byte inline key limit
            const int ValueSize = 50;       // Inline, so each record's only extent is its key

            static byte[] MakeKey(int index)
            {
                var key = new byte[KeySize];
                for (var i = 0; i < KeySize; i++)
                    key[i] = (byte)((index * 7) + i + 1);
                _ = BitConverter.TryWriteBytes(key, index);
                return key;
            }

            static byte[] MakeValue(int index)
            {
                var value = new byte[ValueSize];
                for (var i = 0; i < ValueSize; i++)
                    value[i] = (byte)(index + i);
                return value;
            }

            Guid token;
            using (var log = CreateLogDevice())
            using (var objlog = CreateObjectLogDevice())
            using (var checkpointManager = CreateCheckpointManager())
            {
                var store = CreateOverflowKeyStore(log, objlog, checkpointManager);
                try
                {
                    using (var session = store.NewSession<TestSpanByteKey, TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions>(new TestLargeObjectFunctions()))
                    {
                        for (var index = 0; index < NumKeys; index++)
                            _ = session.BasicContext.Upsert(TestSpanByteKey.FromArray(MakeKey(index)), MakeValue(index).AsSpan(), Empty.Default);
                    }
                    store.Log.FlushAndEvict(wait: true);
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
            using var recoveryCheckpointManager = CreateCheckpointManager();
            var recoverStore = CreateOverflowKeyStore(recoveryLog, countingObjectLog, recoveryCheckpointManager);
            try
            {
                _ = await recoverStore.RecoverAsync(default, token).ConfigureAwait(false);
                var reads = countingObjectLog.ReadCount;
                Assert.That(reads, Is.LessThanOrEqualTo(NumKeys + (NumKeys / 50)),
                    $"{checkpointType} recovery of {NumKeys} overflow keys of {KeySize} bytes took {reads} object-log reads");

                using var session = recoverStore.NewSession<TestSpanByteKey, TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions>(new TestLargeObjectFunctions());
                var context = session.BasicContext;
                for (var index = 0; index < NumKeys; index++)
                {
                    TestLargeObjectInput input = new() { wantValueStyle = TestValueStyle.Inline, expectedSpanLength = ValueSize };
                    TestLargeObjectOutput output = new();
                    var status = context.Read(TestSpanByteKey.FromArray(MakeKey(index)), ref input, ref output, Empty.Default);
                    if (status.IsPending)
                    {
                        Assert.That(context.CompletePendingWithOutputs(out var completedOutputs, wait: true), Is.True);
                        (status, output) = GetSinglePendingResult(completedOutputs);
                    }
                    Assert.That(status.Found, Is.True, $"key {index} not found");
                    Assert.That(output.valueArray, Is.EqualTo(MakeValue(index)), $"key {index} has the wrong value");
                }
            }
            finally
            {
                recoverStore.Dispose();
            }
        }

        /// <summary>
        /// A Snapshot's recovery reads each capture, and each of its own copies of a capture, directly, and the rest of their pages through
        /// the read-ahead. While the checkpoint is held before its flush, CopyUpdates capture the first and a middle record of every page above
        /// ReadOnlyAddress, and the last record of every other one, so some pages end with a copy and others do not. A ReadOnly flush then
        /// writes
        /// the first half of those pages to the main log, with images that point at the captures, so recovery copies those captures to the
        /// main object log's tail; the Snapshot flush writes the rest, whose captures lie below their pages' other records in the checkpoint's
        /// object log.
        /// </summary>
        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task RecoveryReadsCapturesAndCopiesOfThemDirectly()
        {
            Guid token;
            int pagesSpanned, mainLogPages, capturedOnMainLog = 0, capturedInSnapshot = 0;
            using (var log = CreateLogDevice())
            using (var objlog = CreateObjectLogDevice())
            using (var checkpointManager = CreateCheckpointManager())
            {
                var store = CreateStore(log, objlog, checkpointManager);
                try
                {
                    Populate(store);
                    pagesSpanned = PagesSpanned(store);

                    // Pages below ReadOnlyAddress hold no captures, since a ReadOnly flush writes their (v) values; the rest are split between the
                    // logs.
                    var firstCapturedPage = store.hlogBase.GetPage(store.Log.ReadOnlyAddress) + 1;
                    var firstCapturedAddress = store.hlogBase.GetLogicalAddressOfStartOfPage(firstCapturedPage);
                    var mainLogEnd = store.hlogBase.GetLogicalAddressOfStartOfPage((firstCapturedPage + store.hlogBase.GetPage(store.Log.TailAddress)) / 2);
                    mainLogPages = (int)(store.hlogBase.GetPage(mainLogEnd) - store.hlogBase.GetPage(store.Log.BeginAddress));
                    Assert.That(mainLogEnd, Is.GreaterThan(firstCapturedAddress), "some captured pages must be written to the main log");

                    var pages = RecordsByPage(store, firstCapturedAddress, store.Log.TailAddress);
                    var sources = new List<(int key, long address)>();
                    for (var ii = 0; ii < pages.Count; ii++)
                    {
                        var page = pages[ii];
                        sources.Add(page[0]);
                        sources.Add(page[page.Count / 2]);
                        if (ii % 2 == 0)
                            sources.Add(page[^1]);
                    }
                    foreach (var (_, address) in sources)
                    {
                        if (address < mainLogEnd)
                            capturedOnMainLog++;
                        else
                            capturedInSnapshot++;
                    }
                    token = await CaptureDuringCheckpointAsync(store, sources, mainLogEnd).ConfigureAwait(false);
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
                var mainReads = countingObjectLog.ReadCount;
                var snapshotReads = countingFactory.ReadCount;
                TestContext.Progress.WriteLine($"{pagesSpanned} pages, {mainLogPages} in the main log; {capturedOnMainLog} captures copied to the main object log, "
                    + $"{capturedInSnapshot} in the Snapshot; {mainReads} main object-log reads, {snapshotReads} Snapshot object-log reads");

                // Every page's objects are loaded from the main object log, a read or two each, and each copy of a capture there is one more.
                Assert.That(mainReads, Is.LessThanOrEqualTo((2 * pagesSpanned) + capturedOnMainLog),
                    $"recovery loaded the objects of {pagesSpanned} pages, with {capturedOnMainLog} copies of captures, in {mainReads} reads");

                // Recovery reads each capture it copies to the main object log, and copies the Snapshot's pages from its object log, a read or two
                // each, and each capture on them one more.
                var snapshotPages = pagesSpanned - mainLogPages;
                Assert.That(snapshotReads, Is.LessThanOrEqualTo(capturedOnMainLog + (2 * snapshotPages) + capturedInSnapshot),
                    $"recovery copied {snapshotPages} Snapshot pages, with {capturedInSnapshot} captures, in {snapshotReads} reads");

                VerifyAll(recoverStore);
            }
            finally
            {
                recoverStore.Dispose();
            }
        }

        /// <summary>
        /// A scan reads a main-log page whose records are mostly images of captured records with a read for the page's other records. Those
        /// images hold positions in the checkpoint's object log, numbers unrelated to the main object log's, and the walk invalidates them
        /// without reading, so they must not shape its read-ahead.
        /// </summary>
        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task ScanReadsPagesOfCapturedImagesInAReadOrTwo()
        {
            const int Pages = 10;
            using var log = CreateLogDevice();
            using var objectLogFile = CreateObjectLogDevice();
            var countingObjectLog = new ControlledReadFailureDevice(objectLogFile);
            using var checkpointManager = CreateCheckpointManager();
            var store = CreateStore(log, countingObjectLog, checkpointManager);
            try
            {
                Populate(store);

                // Capture all but the first, a middle and the last record of pages above ReadOnlyAddress, then flush them to the main log.
                var scanStart = store.hlogBase.GetLogicalAddressOfStartOfPage(store.hlogBase.GetPage(store.Log.ReadOnlyAddress) + 1);
                var scanEnd = store.hlogBase.GetLogicalAddressOfStartOfPage(store.hlogBase.GetPage(scanStart) + Pages);
                var pages = RecordsByPage(store, scanStart, scanEnd);
                Assert.That(pages, Has.Count.EqualTo(Pages), "the pages to capture");
                var sources = new List<(int key, long address)>();
                var keptCount = 0;
                foreach (var page in pages)
                {
                    for (var ii = 0; ii < page.Count; ii++)
                    {
                        if (ii == 0 || ii == page.Count / 2 || ii == page.Count - 1)
                            keptCount++;
                        else
                            sources.Add(page[ii]);
                    }
                }
                _ = await CaptureDuringCheckpointAsync(store, sources, scanEnd).ConfigureAwait(false);
                store.Log.FlushAndEvict(wait: true);

                var readsBefore = countingObjectLog.ReadCount;
                var scanned = 0;
                using (var iter = store.Log.Scan(scanStart, scanEnd))
                {
                    while (iter.GetNext())
                    {
                        Assert.That(((TestObjectValue)iter.ValueObject).value, Is.EqualTo(iter.Key.AsRef<TestObjectKey>().key), "scanned value");
                        scanned++;
                    }
                }
                var reads = countingObjectLog.ReadCount - readsBefore;
                TestContext.Progress.WriteLine($"{Pages} pages of {sources.Count} captured images and {keptCount} other records; {reads} object-log reads");

                Assert.That(scanned, Is.EqualTo(keptCount), "the scan reports the records that were not captured");
                Assert.That(reads, Is.LessThanOrEqualTo(2 * Pages),
                    $"a scan of {Pages} pages of {sources.Count} captured images and {keptCount} other records took {reads} object-log reads");
            }
            finally
            {
                store.Dispose();
            }
        }

        /// <summary>The key and address of each record of <paramref name="store"/>'s log in [<paramref name="fromAddress"/>,
        /// <paramref name="toAddress"/>), by page.</summary>
        static List<List<(int key, long address)>> RecordsByPage(TsavoriteKV<ClassStoreFunctions, ClassAllocator> store, long fromAddress, long toAddress)
        {
            var pages = new List<List<(int key, long address)>>();
            using var iter = store.Log.Scan(fromAddress, toAddress);
            while (iter.GetNext())
            {
                var address = iter.CurrentAddress;
                if (pages.Count == 0 || store.hlogBase.GetPage(address) != store.hlogBase.GetPage(pages[^1][0].address))
                    pages.Add([]);
                pages[^1].Add((iter.Key.AsRef<TestObjectKey>().key, address));
            }
            return pages;
        }

        /// <summary>
        /// Take a Snapshot checkpoint, holding it before its flush while CopyUpdates of <paramref name="sources"/> capture them and a ReadOnly
        /// flush writes the log below <paramref name="flushUntilAddress"/> to the main log.
        /// </summary>
        /// <returns>The checkpoint's token.</returns>
        static async Task<Guid> CaptureDuringCheckpointAsync(TsavoriteKV<ClassStoreFunctions, ClassAllocator> store, List<(int key, long address)> sources,
            long flushUntilAddress)
        {
            Guid token;
            var pause = new PauseAtPhase(Phase.WAIT_FLUSH);
            store.stateMachineDriver.UnsafeRegisterCallback(pause);
            try
            {
                Assert.That(store.TryInitiateHybridLogCheckpoint(out token, CheckpointType.Snapshot), Is.True);
                Assert.That(pause.Reached.Wait(TimeSpan.FromSeconds(30)), Is.True, "The checkpoint did not reach IN_PROGRESS");
                using (var session = NewSession(store))
                {
                    foreach (var (key, _) in sources)
                    {
                        TestObjectInput input = new() { value = CaptureIncrement };
                        TestObjectOutput output = new();
                        if (session.BasicContext.RMW(new TestObjectKey { key = key }, ref input, ref output).IsPending)
                            _ = session.BasicContext.CompletePending(wait: true);
                    }
                }
                foreach (var (_, address) in sources)
                {
                    Assert.That(store.hlogBase._wrapper.CreateLogRecord(address).Info.HasSnapshotObjectLogReference, Is.True,
                        $"The CopyUpdate of the record at {address} did not capture it");
                }
                store.Log.ShiftReadOnlyAddress(flushUntilAddress, wait: true);
            }
            finally
            {
                pause.Release.Set();
            }
            await store.CompleteCheckpointAsync().ConfigureAwait(false);
            return token;
        }

        /// <summary>Pauses the checkpoint state machine on entry to a chosen phase so the test can act in that window.</summary>
        sealed class PauseAtPhase(Phase pauseAt) : IStateMachineCallback
        {
            internal readonly ManualResetEventSlim Reached = new(false);
            internal readonly ManualResetEventSlim Release = new(false);

            public void BeforeEnteringState(SystemState next)
            {
                if (next.Phase != pauseAt)
                    return;
                Reached.Set();
                _ = Release.Wait(TimeSpan.FromSeconds(30));
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