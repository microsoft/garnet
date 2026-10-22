// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Tsavorite.core;
using static Tsavorite.core.Utility;
using static Tsavorite.test.recovery.CheckpointSharedObjectMutationTests;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.recovery
{
    using ClassAllocator = ObjectAllocator<StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>>;
    using ClassStoreFunctions = StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>;

    /// <summary>
    /// A FoldOver checkpoint ends its flush wherever the tail is, usually mid-sector, and recovers every record below that address
    /// from the main log. The next flush starts in that sector and, because device writes are sector-aligned, writes it again. It
    /// takes the bytes below its start from the image the checkpoint's flush kept, not the live page, so a record there may change in
    /// memory after the checkpoint without changing the checkpoint's image of it. It also waits for the earlier write of that sector,
    /// and after a range was skipped rather than written, it takes the start of the sector from the live page.
    /// </summary>
    [TestFixture]
    internal class FlushBoundarySectorRecoveryTests : TestBase
    {
        public enum SupersedeOp { RMW, Upsert, Delete }

        static readonly int[] OriginalItems = [1, 2, 3];

        [SetUp]
        public void Setup() => DeleteDirectory(MethodTestDir, wait: true);

        [TearDown]
        public void TearDown() => OnTearDown();

        TsavoriteKV<ClassStoreFunctions, ClassAllocator> CreateStore(IDevice logDevice, IDevice objLogDevice)
            => new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = logDevice,
                ObjectLogDevice = objLogDevice,
                MutableFraction = 0.9,
                LogMemorySize = 1L << 16,
                PageSize = 1L << 13,
                CheckpointDir = MethodTestDir
            }, StoreFunctions.Create(new TestObjectKey.Comparer(), () => new SharedListSerializer())
             , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));

        static ClientSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, SharedListFunctions, ClassStoreFunctions, ClassAllocator> NewSession(
                TsavoriteKV<ClassStoreFunctions, ClassAllocator> store, SharedListFunctions functions = null)
            => store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, SharedListFunctions>(functions ?? new SharedListFunctions());

        static void Upsert(TsavoriteKV<ClassStoreFunctions, ClassAllocator> store, int key, params int[] items)
        {
            using var session = NewSession(store);
            _ = session.BasicContext.Upsert(new TestObjectKey { key = key }, new SharedListHeapObject([.. items]), Empty.Default);
        }

        static void Supersede(TsavoriteKV<ClassStoreFunctions, ClassAllocator> store, SupersedeOp op, int key)
        {
            using var session = NewSession(store);
            var context = session.BasicContext;
            switch (op)
            {
                case SupersedeOp.RMW:
                    TestObjectInput input = new() { value = 99 };
                    TestObjectOutput output = new();
                    if (context.RMW(new TestObjectKey { key = key }, ref input, ref output).IsPending)
                        _ = context.CompletePending(wait: true);
                    break;
                case SupersedeOp.Upsert:
                    _ = context.Upsert(new TestObjectKey { key = key }, new SharedListHeapObject([99]), Empty.Default);
                    break;
                case SupersedeOp.Delete:
                    _ = context.Delete(new TestObjectKey { key = key }, Empty.Default);
                    break;
            }
        }

        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task SupersedingABoundarySectorRecordKeepsTheFoldOverImage([Values] SupersedeOp op, [Values] bool flushInFlight)
        {
            using var log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "Boundary.log"), deleteOnClose: false);
            using var objlog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "Boundary.obj.log"), deleteOnClose: false);
            var gatedLog = new GatedCompletionDevice(log);

            var writeStore = CreateStore(gatedLog, objlog);
            try
            {
                var recordAddress = writeStore.Log.TailAddress;
                Upsert(writeStore, 1, OriginalItems);

                Assert.That(writeStore.TryInitiateFullCheckpoint(out _, CheckpointType.FoldOver), Is.True);
                await writeStore.CompleteCheckpointAsync().ConfigureAwait(false);

                var sectorSize = (int)log.SectorSize;
                var flushedUntil = writeStore.hlogBase.FlushedUntilAddress;
                Assert.That(flushedUntil % sectorSize, Is.Not.Zero, "the checkpoint must end mid-sector");
                Assert.That(recordAddress, Is.GreaterThanOrEqualTo(RoundDown(flushedUntil, sectorSize)),
                    "the record must sit in the sector the next flush writes again");

                var heldWrites = 0;
                if (flushInFlight)
                {
                    // Hold the next flush's device writes while the record is superseded. That flush writes the boundary sector on its
                    // own, followed by the rest of its span if the span reaches past that sector.
                    Upsert(writeStore, 2, 7);
                    heldWrites = RoundUp(writeStore.Log.TailAddress, sectorSize) - RoundDown(flushedUntil, sectorSize) > sectorSize ? 2 : 1;
                    gatedLog.Gate = true;
                    writeStore.hlogBase.ShiftReadOnlyAddressWithWait(writeStore.Log.TailAddress, wait: false);
                    gatedLog.WaitForPending(heldWrites, TimeSpan.FromSeconds(30));
                }

                Assert.That(writeStore.hlog.IsFrozenForFlush(recordAddress), Is.False,
                    "a durable record in the boundary sector is not frozen: the next flush writes it from the sector image, not the live page");
                Supersede(writeStore, op, key: 1);

                if (flushInFlight)
                {
                    gatedLog.Gate = false;
                    for (var i = 0; i < heldWrites; i++)
                    {
                        gatedLog.Release(i);
                        gatedLog.WaitForCompletion(i, TimeSpan.FromSeconds(30));
                    }
                }

                // The next flush starts in the boundary sector and writes it again.
                writeStore.Log.ShiftReadOnlyAddress(writeStore.Log.TailAddress, wait: true);
            }
            finally
            {
                writeStore.Dispose();
            }

            var recoverStore = CreateStore(log, objlog);
            try
            {
                _ = await recoverStore.RecoverAsync().ConfigureAwait(false);

                var functions = new SharedListFunctions();
                using var session = NewSession(recoverStore, functions);
                TestObjectInput input = new();
                TestObjectOutput output = new();
                var status = session.BasicContext.Read(new TestObjectKey { key = 1 }, ref input, ref output);
                if (status.IsPending)
                    _ = session.BasicContext.CompletePending(wait: true);

                Assert.That(functions.LastReadAddress, Is.Not.Zero, $"{op}: the FoldOver checkpoint lost key 1, whose record was invalidated in its image");
                Assert.That(functions.LastRead, Is.Not.Null, $"{op}: the FoldOver checkpoint's image of key 1 lost its object");
                Assert.That(functions.LastRead.Items, Is.EqualTo(OriginalItems), $"{op}: the FoldOver checkpoint's image of key 1 changed");
            }
            finally
            {
                recoverStore.Dispose();
            }
        }

        /// <summary>
        /// A flush that starts mid-sector writes that sector again, and devices do not order overlapping writes. So it must not issue its
        /// writes until the previous flush's write of the sector has completed; otherwise that earlier write could land last and replace
        /// the records the later flush stamped there with their image from before it.
        /// </summary>
        [Test]
        [Category("TsavoriteKV")]
        public void AFlushWaitsForThePreviousWriteOfTheSectorItStartsIn()
        {
            using var log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "Ordering.log"), deleteOnClose: true);
            using var objlog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "Ordering.obj.log"), deleteOnClose: true);
            var gatedLog = new GatedCompletionDevice(log);

            var store = CreateStore(gatedLog, objlog);
            Task secondFlush = null;
            var released = 0;
            void ReleaseThrough(int count)
            {
                for (; released < count; released++)
                {
                    gatedLog.Release(released);
                    gatedLog.WaitForCompletion(released, TimeSpan.FromSeconds(30));
                }
            }

            try
            {
                var sectorSize = (int)log.SectorSize;

                // Hold the write of a flush that ends mid-sector.
                Upsert(store, 1, OriginalItems);
                var firstFlushEnd = store.Log.TailAddress;
                Assert.That(firstFlushEnd % sectorSize, Is.Not.Zero, "the first flush must end mid-sector");
                gatedLog.Gate = true;
                store.hlogBase.ShiftReadOnlyAddressWithWait(firstFlushEnd, wait: false);
                gatedLog.WaitForPending(1, TimeSpan.FromSeconds(30));

                // The next flush starts where that one ends. It writes the shared sector on its own, followed by the rest of its span if
                // the span reaches past that sector.
                Upsert(store, 2, 7);
                var secondFlushEnd = store.Log.TailAddress;
                var secondFlushWrites = RoundUp(secondFlushEnd, sectorSize) - RoundDown(firstFlushEnd, sectorSize) > sectorSize ? 2 : 1;
                secondFlush = Task.Run(() => store.hlogBase.ShiftReadOnlyAddressWithWait(secondFlushEnd, wait: false));
                Assert.That(() => store.hlogBase.SafeReadOnlyAddress, Is.EqualTo(secondFlushEnd).After(30_000, 10), "the second flush did not start");

                // Its record walk takes microseconds, so it would have issued its writes well within this interval.
                Thread.Sleep(500);
                Assert.That(gatedLog.CapturedCount, Is.EqualTo(1), "the second flush wrote the sector before the first flush's write of it completed");

                ReleaseThrough(1);
                gatedLog.WaitForPending(1 + secondFlushWrites, TimeSpan.FromSeconds(30));
                gatedLog.Gate = false;
                ReleaseThrough(1 + secondFlushWrites);
                Assert.That(secondFlush.Wait(TimeSpan.FromSeconds(30)), Is.True, "the second flush did not finish issuing its writes");
                Assert.That(() => store.hlogBase.FlushedUntilAddress, Is.EqualTo(secondFlushEnd).After(30_000, 10), "the second flush did not complete");

                store.Log.FlushAndEvict(wait: true);
                int[] key2Items = [7];
                Assert.That(ReadItems(store, 1), Is.EqualTo(OriginalItems), "key 1 is not intact on disk");
                Assert.That(ReadItems(store, 2), Is.EqualTo(key2Items), "key 2 is not intact on disk");
            }
            finally
            {
                // Never leave a write captured, or the second flush and the store's disposal wait for it.
                gatedLog.Gate = false;
                ReleaseThrough(gatedLog.CapturedCount);
                _ = secondFlush?.Wait(TimeSpan.FromSeconds(30));
                store.Dispose();
            }
        }

        /// <summary>
        /// When BeginAddress passes ReadOnlyAddress, the flush of the range below it is skipped, so nothing was written to the sector the
        /// next flush starts in. That flush takes the start of the sector from the live page rather than reading back bytes that were
        /// never written, and nothing reads them because they are below BeginAddress.
        /// </summary>
        [Test]
        [Category("TsavoriteKV")]
        public void AFlushAfterASkippedRangeStartsMidSector()
        {
            using var log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "Skipped.log"), deleteOnClose: true);
            using var objlog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "Skipped.obj.log"), deleteOnClose: true);

            var store = CreateStore(log, objlog);
            try
            {
                const int keyCount = 20;
                var sectorSize = (int)log.SectorSize;
                var addresses = new long[keyCount + 1];
                for (var key = 1; key <= keyCount; key++)
                {
                    addresses[key] = store.Log.TailAddress;
                    Upsert(store, key, key);
                }

                var firstKept = keyCount / 2;
                while (addresses[firstKept] % sectorSize == 0)
                    firstKept++;
                var newBeginAddress = addresses[firstKept];
                Assert.That(newBeginAddress, Is.GreaterThan(store.hlogBase.ReadOnlyAddress), "BeginAddress must pass ReadOnlyAddress");

                store.Log.ShiftBeginAddress(newBeginAddress);
                Assert.That(store.hlogBase.FlushedUntilAddress, Is.EqualTo(newBeginAddress), "the range below BeginAddress should be skipped, not written");

                store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
                Assert.That(store.hlogBase.FlushedUntilAddress, Is.EqualTo(store.Log.TailAddress), "the flush after the skipped range did not complete");

                store.Log.FlushAndEvict(wait: true);
                for (var key = firstKept; key <= keyCount; key++)
                {
                    int[] expectedItems = [key];
                    Assert.That(ReadItems(store, key), Is.EqualTo(expectedItems), $"key {key} is not intact on disk");
                }
            }
            finally
            {
                store.Dispose();
            }
        }

        /// <summary>Reads <paramref name="key"/>'s items, or null if it is not found or has no object.</summary>
        static int[] ReadItems(TsavoriteKV<ClassStoreFunctions, ClassAllocator> store, int key)
        {
            var functions = new SharedListFunctions();
            using var session = NewSession(store, functions);
            TestObjectInput input = new();
            TestObjectOutput output = new();
            if (session.BasicContext.Read(new TestObjectKey { key = key }, ref input, ref output).IsPending)
                _ = session.BasicContext.CompletePending(wait: true);
            return functions.LastRead?.Items.ToArray();
        }
    }
}