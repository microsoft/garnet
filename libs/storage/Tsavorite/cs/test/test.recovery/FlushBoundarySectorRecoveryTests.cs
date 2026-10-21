// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
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
    /// from the main log. The next flush starts in that sector and, because device writes are sector-aligned, rewrites it from the
    /// live page. So a record there must not change in memory after the checkpoint until a flush has moved past its sector, or the
    /// rewrite replaces the checkpoint's image of it.
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
                    "the record must sit in the sector the next flush rewrites");

                if (flushInFlight)
                {
                    // Hold the next flush's device write, which spans the boundary sector, while the record is superseded.
                    Upsert(writeStore, 2, 7);
                    gatedLog.Gate = true;
                    writeStore.hlogBase.ShiftReadOnlyAddressWithWait(writeStore.Log.TailAddress, wait: false);
                    gatedLog.WaitForPending(1, TimeSpan.FromSeconds(30));
                }

                Assert.That(writeStore.hlog.IsFrozenForFlush(recordAddress), Is.True, "the boundary-sector record must be frozen for flush");
                Supersede(writeStore, op, key: 1);

                if (flushInFlight)
                {
                    gatedLog.Release(0);
                    gatedLog.WaitForCompletion(0, TimeSpan.FromSeconds(30));
                    gatedLog.Gate = false;
                }

                // The next flush starts in the boundary sector and rewrites it from the live page.
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
    }
}