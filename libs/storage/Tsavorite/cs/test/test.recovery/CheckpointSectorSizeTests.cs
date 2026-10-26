// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.IO;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using Tsavorite.test.recovery.sumstore;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.recovery
{
    using ClassAllocator = ObjectAllocator<StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>>;
    using ClassStoreFunctions = StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>;
    using StructAllocator = SpanByteAllocator<StoreFunctions<AdId.Comparer, SpanByteRecordTriggers>>;
    using StructStoreFunctions = StoreFunctions<AdId.Comparer, SpanByteRecordTriggers>;

    /// <summary>
    /// Checkpoints and recovery through checkpoint devices whose sector size is larger than the store's log device's, as
    /// when the checkpoint directory is on a filesystem with 4 KiB direct-IO alignment. Every checkpoint device enforces
    /// that alignment (<see cref="StrictSectorDevice"/>), so these tests fail on any platform if the store's log pages,
    /// hash index or overflow buckets are aligned to less than the checkpoint devices need.
    /// </summary>
    [TestFixture]
    public class CheckpointSectorSizeTests : TestBase
    {
        const uint CheckpointSectorSize = StrictSectorNamedDeviceFactoryCreator.DefaultSectorSize;
        const int NumKeys = 5000;
        const int NumObjects = 500;

        [SetUp]
        public void Setup() => DeleteDirectory(MethodTestDir, wait: true);

        [TearDown]
        public void TearDown() => OnTearDown();

        private static DeviceLogCommitCheckpointManager CreateCheckpointManager(INamedDeviceFactoryCreator creator)
            => new(creator, new DefaultCheckpointNamingScheme(MethodTestDir));

        private static TsavoriteKV<StructStoreFunctions, StructAllocator> CreateSpanByteStore(IDevice logDevice, ICheckpointManager checkpointManager,
                long indexSize = 1L << 16, long pageSize = 1L << 14, long logMemorySize = 1L << 22)
            => new(new()
            {
                IndexSize = indexSize,
                LogDevice = logDevice,
                PageSize = pageSize,
                LogMemorySize = logMemorySize,
                MutableFraction = 0.1,
                CheckpointManager = checkpointManager
            }, StoreFunctions.Create(new AdId.Comparer(), SpanByteRecordTriggers.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));

        private static TsavoriteKV<ClassStoreFunctions, ClassAllocator> CreateObjectStore(ICheckpointManager checkpointManager)
            => new(new()
            {
                IndexSize = 1L << 16,
                LogDevice = new NullDevice(),
                PageSize = 1L << 16,
                LogMemorySize = 1L << 22,
                CheckpointManager = checkpointManager
            }, StoreFunctions.Create(new TestObjectKey.Comparer(), () => new TestLargeObjectValue.Serializer())
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));

        /// <summary>Assert the store adopted <paramref name="sectorSize"/> for its log pages, and aligned its hash index and
        /// overflow buckets to it; these are the buffers checkpoints write from and recovery reads into.</summary>
        private static unsafe void AssertStoreSectorSize<TStoreFunctions, TAllocator>(TsavoriteKV<TStoreFunctions, TAllocator> store, uint sectorSize)
            where TStoreFunctions : IStoreFunctions
            where TAllocator : IAllocator<TStoreFunctions>
        {
            ClassicAssert.AreEqual(sectorSize, (uint)store.hlogBase.GetDeviceSectorSize(), "log sector size");
            AssertIndexAligned(store, sectorSize);
        }

        private static unsafe void AssertIndexAligned(TsavoriteBase store, uint sectorSize)
        {
            ClassicAssert.AreEqual(0L, (long)store.state[store.resizeInfo.version].tableAligned % sectorSize, "hash index table alignment");
            ClassicAssert.AreEqual(0L, store.overflowBucketsAllocator.GetPhysicalAddress(0) % sectorSize, "overflow bucket level 0 alignment");
            ClassicAssert.AreEqual(0L, store.overflowBucketsAllocator.GetPhysicalAddress(1L << 16) % sectorSize, "overflow bucket level 1 alignment");
        }

        private static void AssertIoWentThroughStrictDevices(StrictSectorNamedDeviceFactoryCreator creator)
        {
            ClassicAssert.Greater(creator.RequestCount, 0, "no checkpoint IO reached the strict-sector devices");
            ClassicAssert.AreEqual(0, creator.MisalignedRequestCount, "misaligned checkpoint IO");
        }

        /// <summary>
        /// A store checkpoints to and recovers from devices with a larger sector size than its log device: an in-memory
        /// store (whose <see cref="NullDevice"/> has the minimum sector size), and a store whose log is on a device with the
        /// minimum sector size. A hash index smaller than one checkpoint sector also overflows into overflow buckets.
        /// </summary>
        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        [TestCase(false, CheckpointType.Snapshot, 1L << 10)]
        [TestCase(false, CheckpointType.Snapshot, 1L << 16)]
        [TestCase(true, CheckpointType.Snapshot, 1L << 16)]
        [TestCase(true, CheckpointType.FoldOver, 1L << 10)]
        [TestCase(true, CheckpointType.FoldOver, 1L << 16)]
        public async Task SpanByteStoreRoundTripsThroughLargerCheckpointSector(bool logOnDevice, CheckpointType checkpointType, long indexSize)
        {
            var creator = new StrictSectorNamedDeviceFactoryCreator();
            var logPath = Path.Join(MethodTestDir, "hlog.log");

            // A store whose log is on a device spills most records to it, so recovery reads them back from that device.
            var logMemorySize = logOnDevice ? 1L << 15 : 1L << 22;

            IDevice log = logOnDevice ? Devices.CreateLogDevice(logPath) : new NullDevice();
            try
            {
                using var checkpointManager = CreateCheckpointManager(creator);
                using var store = CreateSpanByteStore(log, checkpointManager, indexSize, logMemorySize: logMemorySize);
                AssertStoreSectorSize(store, CheckpointSectorSize);

                using (var session = store.NewSession<AdId, AdInput, Output, Empty, AdSimpleFunctions>(new AdSimpleFunctions()))
                {
                    var bContext = session.BasicContext;
                    for (long key = 0; key < NumKeys; key++)
                    {
                        var value = key;
                        _ = bContext.Upsert(new AdId { adId = key }, SpanByte.FromPinnedVariable(ref value), Empty.Default);
                    }
                }
                if (indexSize < CheckpointSectorSize)
                    ClassicAssert.Greater(store.overflowBucketsAllocator.NumAllocations, 0, "a table smaller than a sector must spill the keys into overflow buckets");

                ClassicAssert.IsTrue(store.TryInitiateFullCheckpoint(out _, checkpointType));
                await store.CompleteCheckpointAsync();
            }
            finally
            {
                log.Dispose();
            }
            AssertIoWentThroughStrictDevices(creator);

            log = logOnDevice ? Devices.CreateLogDevice(logPath) : new NullDevice();
            try
            {
                using var checkpointManager = CreateCheckpointManager(creator);
                using var store = CreateSpanByteStore(log, checkpointManager, indexSize, logMemorySize: logMemorySize);
                _ = await store.RecoverAsync();
                AssertStoreSectorSize(store, CheckpointSectorSize);

                using var session = store.NewSession<AdId, AdInput, Output, Empty, AdSimpleFunctions>(new AdSimpleFunctions());
                var bContext = session.BasicContext;
                AdInput input = default;
                for (long key = 0; key < NumKeys; key++)
                {
                    Output output = default;
                    var status = bContext.Read(new AdId { adId = key }, ref input, ref output, Empty.Default);
                    if (status.IsPending)
                    {
                        _ = bContext.CompletePendingWithOutputs(out var outputs, wait: true);
                        ClassicAssert.IsTrue(outputs.Next());
                        status = outputs.Current.Status;
                        output = outputs.Current.Output;
                        outputs.Current.Dispose();
                        ClassicAssert.IsFalse(outputs.Next());
                    }
                    ClassicAssert.IsTrue(status.Found, $"key {key} not found after recovery");
                    ClassicAssert.AreEqual(key, output.value.numClicks, $"key {key}");
                }
            }
            finally
            {
                log.Dispose();
            }
            ClassicAssert.AreEqual(0, creator.MisalignedRequestCount, "misaligned recovery IO");
        }

        /// <summary>
        /// An in-memory object store snapshots its objects to an object-log device with a larger sector size than its log
        /// device, and recovers them.
        /// </summary>
        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task InMemoryObjectStoreRoundTripsThroughLargerCheckpointSector()
        {
            var creator = new StrictSectorNamedDeviceFactoryCreator();

            using (var checkpointManager = CreateCheckpointManager(creator))
            using (var store = CreateObjectStore(checkpointManager))
            {
                AssertStoreSectorSize(store, CheckpointSectorSize);

                using (var session = store.NewSession<TestObjectKey, TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions>(new TestLargeObjectFunctions()))
                {
                    var bContext = session.BasicContext;
                    for (var key = 0; key < NumObjects; key++)
                        _ = bContext.Upsert(new TestObjectKey { key = key }, new TestLargeObjectValue(ObjectSize(key)), Empty.Default);
                }

                ClassicAssert.IsTrue(store.TryInitiateFullCheckpoint(out _, CheckpointType.Snapshot));
                await store.CompleteCheckpointAsync();
            }
            AssertIoWentThroughStrictDevices(creator);

            using (var checkpointManager = CreateCheckpointManager(creator))
            using (var store = CreateObjectStore(checkpointManager))
            {
                _ = await store.RecoverAsync();

                using var session = store.NewSession<TestObjectKey, TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions>(new TestLargeObjectFunctions());
                var bContext = session.BasicContext;
                TestLargeObjectInput input = new() { wantValueStyle = TestValueStyle.Object };
                for (var key = 0; key < NumObjects; key++)
                {
                    TestLargeObjectOutput output = new();
                    var status = bContext.Read(new TestObjectKey { key = key }, ref input, ref output, Empty.Default);
                    if (status.IsPending)
                        (status, output) = bContext.GetSinglePendingResult();
                    ClassicAssert.IsTrue(status.Found, $"key {key} not found after recovery");

                    var expected = new TestLargeObjectValue(ObjectSize(key)).value;
                    CollectionAssert.AreEqual(expected, output.valueObject.value, $"key {key}");
                }
            }
            ClassicAssert.AreEqual(0, creator.MisalignedRequestCount, "misaligned recovery IO");

            static int ObjectSize(int key) => 100 + (key * 37 % 3000);
        }

        /// <summary>
        /// A checkpoint manager that does not report the sector size its devices need leaves the store at its log device's
        /// sector size; the checkpoint then fails up front with an explicit error rather than with misaligned IO.
        /// </summary>
        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public void CheckpointFailsBeforeIoWhenCheckpointSectorIsNotReported()
        {
            var creator = new StrictSectorNamedDeviceFactoryCreator(reportSectorSize: false);
            using var checkpointManager = CreateCheckpointManager(creator);
            using var store = CreateSpanByteStore(new NullDevice(), checkpointManager);
            ClassicAssert.AreEqual(IDevice.MinDeviceSectorSize, store.hlogBase.GetDeviceSectorSize());

            using (var session = store.NewSession<AdId, AdInput, Output, Empty, AdSimpleFunctions>(new AdSimpleFunctions()))
            {
                var value = 1L;
                _ = session.BasicContext.Upsert(new AdId { adId = 1 }, SpanByte.FromPinnedVariable(ref value), Empty.Default);
            }

            // Run the state machine inline so its failure is observed deterministically; a registered one can finish and
            // clear its completion before CompleteCheckpointAsync looks for it.
            var ex = Assert.CatchAsync<TsavoriteException>(async ()
                => await store.stateMachineDriver.RunAsync(Checkpoint.Full(store, CheckpointType.Snapshot, out _)));
            StringAssert.Contains($"device with sector size {CheckpointSectorSize}", ex.Message);
            ClassicAssert.AreEqual(0, creator.MisalignedRequestCount, "the checkpoint must fail before issuing misaligned IO");
        }

        /// <summary>
        /// A checkpoint device whose sector size exceeds the page size cannot be served, so the store keeps its log
        /// device's sector size instead of failing to start; only a checkpoint would fail.
        /// </summary>
        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public void StoreStartsWhenCheckpointSectorExceedsPageSize()
        {
            var creator = new StrictSectorNamedDeviceFactoryCreator(sectorSize: 2 * CheckpointSectorSize);
            using var checkpointManager = CreateCheckpointManager(creator);
            using var store = CreateSpanByteStore(new NullDevice(), checkpointManager, pageSize: CheckpointSectorSize);
            ClassicAssert.AreEqual(IDevice.MinDeviceSectorSize, store.hlogBase.GetDeviceSectorSize());
        }

        /// <summary>
        /// The hash index and every overflow-bucket page are aligned to the sector size the index is initialized with, and
        /// each overflow-bucket page carries a sector of slack for that alignment.
        /// </summary>
        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public void HashIndexAndOverflowBucketsFollowTheIndexSectorSize([Values(512, 4096, 65536)] int sectorSize)
        {
            var table = new TsavoriteBase();
            try
            {
                table.Initialize(1L << 10, sectorSize);
                ClassicAssert.AreEqual(sectorSize, table.overflowBucketsAllocator.SectorSize);
                AssertIndexAligned(table, (uint)sectorSize);

                // Levels 0 and 1 are committed at construction.
                ClassicAssert.AreEqual(2 * (MallocFixedPageSize<HashBucket>.MemorySizeGranularity + sectorSize), table.overflowBucketsAllocator.AllocatedSizeBytes);
            }
            finally
            {
                table.Free();
            }
        }

        /// <summary>
        /// A fresh store's overflow buckets commit exactly its overflow floor, at the sector size it adopted from its
        /// checkpoint devices; an index memory budget adds that floor to the configured index size. The floor exceeds
        /// <see cref="KVSettings.OverflowBucketFloorMemorySize"/>, which assumes the minimum sector size, only when the
        /// store's sector size is larger.
        /// </summary>
        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public void OverflowFloorIsWhatAFreshStoreCommits([Values] bool largerCheckpointSector)
        {
            var creator = new StrictSectorNamedDeviceFactoryCreator(reportSectorSize: largerCheckpointSector);
            using var checkpointManager = CreateCheckpointManager(creator);
            using var store = CreateSpanByteStore(new NullDevice(), checkpointManager);

            var sectorSize = largerCheckpointSector ? (int)CheckpointSectorSize : IDevice.MinDeviceSectorSize;
            ClassicAssert.AreEqual(sectorSize, store.hlogBase.GetDeviceSectorSize());
            ClassicAssert.AreEqual(store.IndexOverflowFloorSizeBytes, store.IndexOverflowSizeBytes);
            ClassicAssert.AreEqual(2 * (MallocFixedPageSize<HashBucket>.MemorySizeGranularity + sectorSize), store.IndexOverflowFloorSizeBytes);
            if (largerCheckpointSector)
                ClassicAssert.Greater(store.IndexOverflowFloorSizeBytes, KVSettings.OverflowBucketFloorMemorySize);
            else
                ClassicAssert.AreEqual(KVSettings.OverflowBucketFloorMemorySize, store.IndexOverflowFloorSizeBytes);
        }

        /// <summary>
        /// A local-storage device factory reports the sector size of the devices it would create, for each device type,
        /// without creating the device's directory; so does the checkpoint manager built on it.
        /// </summary>
        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public void LocalStorageFactoryReportsSectorSizeWithoutCreatingDirectories(
            [Values(DeviceType.Default, DeviceType.RandomAccess, DeviceType.FileStream, DeviceType.LocalMemory)] DeviceType deviceType)
        {
            var baseDir = Path.Join(MethodTestDir, "probe");
            var fileInfo = new FileDescriptor("sub", "file.dat");
            var factory = new LocalStorageNamedDeviceFactoryCreator(deviceType: deviceType).Create(baseDir);

            var probed = factory.GetSectorSize(fileInfo);
            var managerProbed = CreateManagerSectorSize(deviceType, baseDir);
            ClassicAssert.IsFalse(Directory.Exists(baseDir), "probing must not create the directory");

            using var device = factory.Get(fileInfo);
            ClassicAssert.AreEqual(device.SectorSize, probed);
            ClassicAssert.AreEqual(device.SectorSize, managerProbed);

            static uint CreateManagerSectorSize(DeviceType deviceType, string baseDir)
            {
                using var manager = new DeviceLogCommitCheckpointManager(new LocalStorageNamedDeviceFactoryCreator(deviceType: deviceType),
                    new DefaultCheckpointNamingScheme(baseDir));
                return manager.GetCheckpointDeviceSectorSize();
            }
        }

        /// <summary>
        /// Factories and checkpoint managers that do not implement the sector-size members add no alignment requirement.
        /// </summary>
        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public void SectorSizeMembersDefaultToTheMinimum()
        {
            INamedDeviceFactory factory = new NullNamedDeviceFactoryCreator().Create(MethodTestDir);
            ClassicAssert.AreEqual((uint)IDevice.MinDeviceSectorSize, factory.GetSectorSize(new FileDescriptor("sub", "file.dat")));

            using var deviceCheckpointManager = new SingleDeviceCheckpointManager(new NullDevice());
            ICheckpointManager checkpointManager = deviceCheckpointManager;
            ClassicAssert.AreEqual((uint)IDevice.MinDeviceSectorSize, checkpointManager.GetCheckpointDeviceSectorSize());
        }
    }
}