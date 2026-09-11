// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using Tsavorite.devices;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test
{
    using StructAllocator = SpanByteAllocator<StoreFunctions<KeyStruct.Comparer, SpanByteRecordTriggers>>;
    using StructStoreFunctions = StoreFunctions<KeyStruct.Comparer, SpanByteRecordTriggers>;
    [TestFixture]
    internal class BasicStorageTests : TestBase
    {
        [Test]
        [Category("TsavoriteKV")]
        public void LocalStorageWriteRead()
        {
            TestDeviceWriteRead(Devices.CreateLogDevice(Path.Join(MethodTestDir, "BasicDiskTests.log"), deleteOnClose: true));
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void PageBlobWriteRead()
        {
            IgnoreIfNotRunningAzureTests();
            TestDeviceWriteRead(new AzureStorageDevice(AzureEmulatedStorageString, AzureTestContainer, AzureTestDirectory, "BasicDiskTests", logger: TestLoggerFactory.CreateLogger("asd")));
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void PageBlobWriteReadWithLease()
        {
            IgnoreIfNotRunningAzureTests();
            TestDeviceWriteRead(new AzureStorageDevice(AzureEmulatedStorageString, AzureTestContainer, AzureTestDirectory, "BasicDiskTests", null, true, true, logger: TestLoggerFactory.CreateLogger("asd")));
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void TieredWriteRead()
        {
            DeleteDirectory(MethodTestDir);
            IDevice tested;
            IDevice localDevice = Devices.CreateLogDevice(Path.Join(MethodTestDir, "BasicDiskTests.log"), deleteOnClose: true, capacity: 1L << 30);
            if (IsRunningAzureTests)
            {
                IDevice cloudDevice = new AzureStorageDevice(AzureEmulatedStorageString, AzureTestContainer, AzureTestDirectory, "BasicDiskTests", logger: TestLoggerFactory.CreateLogger("asd"));
                tested = new TieredStorageDevice(1, localDevice, cloudDevice);
            }
            else
            {
                // If no Azure is enabled, just use another disk
                IDevice localDevice2 = Devices.CreateLogDevice(Path.Join(MethodTestDir, "BasicDiskTests2.log"), deleteOnClose: true, capacity: 1L << 30);
                tested = new TieredStorageDevice(1, localDevice, localDevice2);

            }
            TestDeviceWriteRead(tested);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void ShardedWriteRead()
        {
            IDevice localDevice1 = Devices.CreateLogDevice(Path.Join(MethodTestDir, "BasicDiskTests1.log"), deleteOnClose: true, capacity: 1L << 30);
            IDevice localDevice2 = Devices.CreateLogDevice(Path.Join(MethodTestDir, "BasicDiskTests2.log"), deleteOnClose: true, capacity: 1L << 30);
            var device = new ShardedStorageDevice(new UniformPartitionScheme(IDevice.MinDeviceSectorSize, localDevice1, localDevice2));
            TestDeviceWriteRead(device);
        }

        /// <summary>
        /// A shard whose read throws synchronously must still produce exactly one aggregate completion. Letting the
        /// exception unwind would strand the shards already issued: the caller is told nothing, yet those reads keep
        /// writing into the destination buffer that the caller is then free to reuse or free.
        /// </summary>
        [Test]
        [Category("TsavoriteKV")]
        public unsafe void ShardedReadCompletesWhenLaterShardThrows()
        {
            const int SectorSize = IDevice.MinDeviceSectorSize;

            var localDevice1 = Devices.CreateLogDevice(Path.Join(MethodTestDir, "ShardedThrow1.log"), deleteOnClose: true, capacity: 1L << 30);
            var throwingDevice = new SyncThrowOnReadDevice(Devices.CreateLogDevice(Path.Join(MethodTestDir, "ShardedThrow2.log"), deleteOnClose: true, capacity: 1L << 30));
            using var device = new ShardedStorageDevice(new UniformPartitionScheme(SectorSize, localDevice1, throwingDevice));
            device.Initialize(segmentSize: 1L << 30, epoch: null);

            // The read spans both shards, so the first shard is issued before the second one throws.
            var buffer = (byte*)NativeMemory.AlignedAlloc(2 * SectorSize, SectorSize);
            try
            {
                NativeMemory.Clear(buffer, 2 * SectorSize);

                using var writeDone = new ManualResetEventSlim(false);
                device.WriteAsync((IntPtr)buffer, 0, 0, 2 * SectorSize, (_, _, _, _) => writeDone.Set(), null);
                ClassicAssert.IsTrue(writeDone.Wait(TimeSpan.FromSeconds(30)), "Write did not complete");

                throwingDevice.ArmReadFailure = true;

                var callbackCount = 0;
                uint observedErrorCode = 0;
                using var readDone = new ManualResetEventSlim(false);
                Assert.DoesNotThrow(() => device.ReadAsync(0, 0, (IntPtr)buffer, 2 * SectorSize,
                    (errorCode, _, _, _) =>
                    {
                        _ = Interlocked.Increment(ref callbackCount);
                        observedErrorCode = errorCode;
                        readDone.Set();
                    }, null));

                ClassicAssert.IsTrue(readDone.Wait(TimeSpan.FromSeconds(30)), "Aggregate read completion never fired after a shard threw");
                ClassicAssert.AreNotEqual(0u, observedErrorCode, "A failed shard read must be reported as an error");

                // Give any stray completion a chance to arrive before asserting the callback fired exactly once.
                Thread.Sleep(250);
                ClassicAssert.AreEqual(1, Volatile.Read(ref callbackCount), "Aggregate read completion must fire exactly once");
            }
            finally
            {
                NativeMemory.AlignedFree(buffer);
            }
        }

        /// <summary>
        /// The write counterpart of <see cref="ShardedReadCompletesWhenLaterShardThrows"/>. A shard that throws
        /// synchronously from WriteAsync was never issued, so nothing will ever signal the countdown slot taken for it.
        /// The aggregate write must still complete exactly once and report the error, because shards issued before it
        /// are still reading from the caller's buffer; an operation that never completes would leave the caller free to
        /// reuse or free that buffer while those writes are in flight.
        /// </summary>
        [Test]
        [Category("TsavoriteKV")]
        public unsafe void ShardedWriteCompletesWhenLaterShardThrows()
        {
            const int SectorSize = IDevice.MinDeviceSectorSize;

            var localDevice1 = Devices.CreateLogDevice(Path.Join(MethodTestDir, "ShardedWriteThrow1.log"), deleteOnClose: true, capacity: 1L << 30);
            var throwingDevice = new SyncThrowOnWriteDevice(Devices.CreateLogDevice(Path.Join(MethodTestDir, "ShardedWriteThrow2.log"), deleteOnClose: true, capacity: 1L << 30));
            using var device = new ShardedStorageDevice(new UniformPartitionScheme(SectorSize, localDevice1, throwingDevice));
            device.Initialize(segmentSize: 1L << 30, epoch: null);

            // The write spans both shards, so the first shard is issued before the second one throws.
            var buffer = (byte*)NativeMemory.AlignedAlloc(2 * SectorSize, SectorSize);
            try
            {
                NativeMemory.Clear(buffer, 2 * SectorSize);
                throwingDevice.ArmWriteFailure = true;

                var callbackCount = 0;
                uint observedErrorCode = 0;
                using var writeDone = new ManualResetEventSlim(false);
                Assert.DoesNotThrow(() => device.WriteAsync((IntPtr)buffer, 0, 0, 2 * SectorSize,
                    (errorCode, _, _, _) =>
                    {
                        _ = Interlocked.Increment(ref callbackCount);
                        observedErrorCode = errorCode;
                        writeDone.Set();
                    }, null));

                ClassicAssert.IsTrue(writeDone.Wait(TimeSpan.FromSeconds(30)), "Aggregate write completion never fired after a shard threw");
                ClassicAssert.AreNotEqual(0u, observedErrorCode, "A failed shard write must be reported as an error");

                // Give any stray completion a chance to arrive before asserting the callback fired exactly once.
                Thread.Sleep(250);
                ClassicAssert.AreEqual(1, Volatile.Read(ref callbackCount), "Aggregate write completion must fire exactly once");
            }
            finally
            {
                NativeMemory.AlignedFree(buffer);
            }
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void OmitSegmentIdTest([Values] TestUtils.TestDeviceType deviceType)
        {
            var filename = Path.Join(MethodTestDir, "test.log");
            var omit = false;
            for (var ii = 0; ii < 2; ++ii)
            {
                using IDevice device = CreateTestDevice(deviceType, filename, omitSegmentIdFromFilename: omit);
                var storageBase = (StorageDeviceBase)device;
                var segmentFilename = storageBase.GetSegmentFilename(filename, 0);
                if (omit)
                    ClassicAssert.AreEqual(filename, segmentFilename);
                else
                    ClassicAssert.AreEqual(filename + ".0", segmentFilename);
                omit = true;
            }
        }

        static void TestDeviceWriteRead(IDevice log)
        {
            var store = new TsavoriteKV<StructStoreFunctions, StructAllocator>(
                new()
                {
                    IndexSize = 1L << 26,
                    LogDevice = log,
                    LogMemorySize = 1L << 15,
                    PageSize = MinKvLogPageSize,
                }, StoreFunctions.Create(KeyStruct.Comparer.Instance, SpanByteRecordTriggers.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            var session = store.NewSession<KeyStruct, InputStruct, OutputStruct, Empty, Functions>(new Functions());
            var bContext = session.BasicContext;

            InputStruct input = default;

            for (int i = 0; i < 700; i++)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                _ = bContext.Upsert(key1, SpanByte.FromPinnedVariable(ref value), Empty.Default);
            }
            _ = bContext.CompletePending(true);

            // Update first 100 using RMW from storage
            for (int i = 0; i < 100; i++)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                input = new InputStruct { ifield1 = 1, ifield2 = 1 };
                var status = bContext.RMW(key1, ref input, Empty.Default);
                if (status.IsPending)
                    _ = bContext.CompletePending(true);
            }


            for (int i = 0; i < 700; i++)
            {
                OutputStruct output = default;
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                if (bContext.Read(key1, ref input, ref output, Empty.Default).IsPending)
                {
                    _ = bContext.CompletePending(true);
                }
                else
                {
                    if (i < 100)
                    {
                        ClassicAssert.AreEqual(value.vfield1 + 1, output.value.vfield1);
                        ClassicAssert.AreEqual(value.vfield2 + 1, output.value.vfield2);
                    }
                    else
                    {
                        ClassicAssert.AreEqual(value.vfield1, output.value.vfield1);
                        ClassicAssert.AreEqual(value.vfield2, output.value.vfield2);
                    }
                }
            }

            session.Dispose();
            store.Dispose();
            store = null;
            log.Dispose();
            DeleteDirectory(MethodTestDir);
        }
    }
}