// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using Allure.NUnit;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test
{
    // Must be in a separate block so the "using StructStoreFunctions" is the first line in its namespace declaration.
    struct KeyStructComparerModulo : IKeyComparer<KeyStruct>
    {
        readonly long mod;

        internal KeyStructComparerModulo(long mod) => this.mod = mod;

        public readonly bool Equals(ref KeyStruct k1, ref KeyStruct k2) => k1.kfield1 == k2.kfield1 && k1.kfield2 == k2.kfield2;

        // Force collisions to create a chain
        public readonly long GetHashCode64(ref KeyStruct key)
        {
            long hash = Utility.GetHashCode(key.kfield1);
            return mod > 0 ? hash % mod : hash;
        }
    }
}

namespace Tsavorite.test
{
    using StructAllocator = BlittableAllocator<KeyStruct, ValueStruct, StoreFunctions<KeyStruct, ValueStruct, KeyStructComparerModulo, DefaultRecordDisposer<KeyStruct, ValueStruct>>>;
    using StructStoreFunctions = StoreFunctions<KeyStruct, ValueStruct, KeyStructComparerModulo, DefaultRecordDisposer<KeyStruct, ValueStruct>>;

    [AllureNUnit]
    [TestFixture]
    internal class BlittableLogScanTests : AllureTestBase
    {
        private TsavoriteKV<KeyStruct, ValueStruct, StructStoreFunctions, StructAllocator> store;
        private IDevice log;
        const int TotalRecords = 2000;
        const int PageSizeBits = 10;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);

            KeyStructComparerModulo comparer = new(0);
            foreach (var arg in TestContext.CurrentContext.Test.Arguments)
            {
                if (arg is HashModulo mod && mod == HashModulo.Hundred)
                {
                    comparer = new(100);
                    continue;
                }
            }

            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "test.log"), deleteOnClose: true);
            store = new(new()
            {
                IndexSize = 1L << 26,
                LogDevice = log,
                MemorySize = 1L << 24,
                PageSize = 1L << PageSizeBits
            }, StoreFunctions<KeyStruct, ValueStruct>.Create(comparer)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );
        }

        [TearDown]
        public void TearDown()
        {
            store?.Dispose();
            store = null;
            log?.Dispose();
            log = null;
            DeleteDirectory(MethodTestDir);
        }

        internal struct BlittablePushScanTestFunctions : IScanIteratorFunctions<KeyStruct, ValueStruct>
        {
            internal long numRecords;

            public readonly bool OnStart(long beginAddress, long endAddress) => true;

            public bool ConcurrentReader(ref KeyStruct key, ref ValueStruct value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                => SingleReader(ref key, ref value, recordMetadata, numberOfRecords, out cursorRecordResult);

            public bool SingleReader(ref KeyStruct key, ref ValueStruct value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
            {
                cursorRecordResult = CursorRecordResult.Accept; // default; not used here
                ClassicAssert.AreEqual(numRecords, key.kfield1);
                ClassicAssert.AreEqual(numRecords + 1, key.kfield2);
                ClassicAssert.AreEqual(numRecords, value.vfield1);
                ClassicAssert.AreEqual(numRecords + 1, value.vfield2);

                ++numRecords;
                return true;
            }

            public readonly void OnException(Exception exception, long numberOfRecords) { }

            public readonly void OnStop(bool completed, long numberOfRecords) { }
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]

        public void BlittableDiskWriteScan([Values] ScanIteratorType scanIteratorType)
        {
            using var session = store.NewSession<InputStruct, OutputStruct, Empty, Functions>(new Functions());
            var bContext = session.BasicContext;

            using var s = store.Log.Subscribe(new LogObserver());
            var start = store.Log.TailAddress;

            for (int i = 0; i < TotalRecords; i++)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                _ = bContext.Upsert(ref key1, ref value, Empty.Default);
            }
            store.Log.FlushAndEvict(true);

            BlittablePushScanTestFunctions scanIteratorFunctions = new();
            void scanAndVerify(ScanBufferingMode sbm)
            {
                scanIteratorFunctions.numRecords = 0;

                if (scanIteratorType == ScanIteratorType.Pull)
                {
                    using var iter = store.Log.Scan(start, store.Log.TailAddress, sbm);
                    while (iter.GetNext(out var recordInfo))
                        _ = scanIteratorFunctions.SingleReader(ref iter.GetKey(), ref iter.GetValue(), default, default, out _);
                }
                else
                    ClassicAssert.IsTrue(store.Log.Scan(ref scanIteratorFunctions, start, store.Log.TailAddress, sbm), "Failed to complete push iteration");

                ClassicAssert.AreEqual(TotalRecords, scanIteratorFunctions.numRecords);
            }

            scanAndVerify(ScanBufferingMode.SinglePageBuffering);
            scanAndVerify(ScanBufferingMode.DoublePageBuffering);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]

        public void BlittableScanJumpToBeginAddressTest()
        {
            using var session = store.NewSession<InputStruct, OutputStruct, Empty, Functions>(new Functions());
            var bContext = session.BasicContext;

            const int numRecords = 200;
            const int numTailRecords = 10;
            long shiftBeginAddressTo = 0;
            int shiftToKey = 0;
            for (int i = 0; i < numRecords; i++)
            {
                if (i == numRecords - numTailRecords)
                {
                    shiftBeginAddressTo = store.Log.TailAddress;
                    shiftToKey = i;
                }
                var key = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                _ = bContext.Upsert(ref key, ref value, Empty.Default);
            }

            using var iter = store.Log.Scan(store.Log.HeadAddress, store.Log.TailAddress);

            for (int i = 0; i < 100; ++i)
            {
                ClassicAssert.IsTrue(iter.GetNext(out var recordInfo));
                ClassicAssert.AreEqual(i, iter.GetKey().kfield1);
                ClassicAssert.AreEqual(i, iter.GetValue().vfield1);
            }

            store.Log.ShiftBeginAddress(shiftBeginAddressTo);

            for (int i = 0; i < numTailRecords; ++i)
            {
                ClassicAssert.IsTrue(iter.GetNext(out var recordInfo));
                if (i == 0)
                    ClassicAssert.AreEqual(store.Log.BeginAddress, iter.CurrentAddress);
                var expectedKey = numRecords - numTailRecords + i;
                ClassicAssert.AreEqual(expectedKey, iter.GetKey().kfield1);
                ClassicAssert.AreEqual(expectedKey, iter.GetValue().vfield1);
            }
        }

        public class ScanFunctions : FunctionsWithContext<Empty>
        {
            // Right now this is unused but helped with debugging so I'm keeping it around.
            internal long insertedAddress;

            public override bool SingleWriter(ref KeyStruct key, ref InputStruct input, ref ValueStruct src, ref ValueStruct dst, ref OutputStruct output, ref UpsertInfo upsertInfo, WriteReason reason, ref RecordInfo recordInfo)
            {
                insertedAddress = upsertInfo.Address;
                return base.SingleWriter(ref key, ref input, ref src, ref dst, ref output, ref upsertInfo, reason, ref recordInfo);
            }
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]

        public void BlittableScanCursorTest([Values(HashModulo.NoMod, HashModulo.Hundred)] HashModulo hashMod)
        {
            const long PageSize = 1L << PageSizeBits;
            var recordSize = BlittableAllocatorImpl<KeyStruct, ValueStruct, StructStoreFunctions>.RecordSize;

            using var session = store.NewSession<InputStruct, OutputStruct, Empty, ScanFunctions>(new ScanFunctions());
            var bContext = session.BasicContext;

            for (int i = 0; i < TotalRecords; i++)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                _ = bContext.Upsert(ref key1, ref value);
            }

            var scanCursorFuncs = new ScanCursorFuncs();

            // Normal operations
            var endAddresses = new long[] { store.Log.TailAddress, long.MaxValue };
            var counts = new long[] { 10, 100, long.MaxValue };

            long cursor = 0;
            for (var iAddr = 0; iAddr < endAddresses.Length; ++iAddr)
            {
                for (var iCount = 0; iCount < counts.Length; ++iCount)
                {
                    scanCursorFuncs.Initialize(verifyKeys: true);
                    while (session.ScanCursor(ref cursor, counts[iCount], scanCursorFuncs, endAddresses[iAddr]))
                        ;
                    ClassicAssert.AreEqual(TotalRecords, scanCursorFuncs.numRecords, $"count: {counts[iCount]}, endAddress {endAddresses[iAddr]}");
                    ClassicAssert.AreEqual(0, cursor, "Expected cursor to be 0, pt 1");
                }
            }

            // After FlushAndEvict, we will be doing pending IO. With collision chains, this means we may be returning colliding keys from in-memory
            // before the sequential keys from pending IO. Therefore we do not want to verify keys if we are causing collisions.
            store.Log.FlushAndEvict(wait: true);
            bool verifyKeys = hashMod == HashModulo.NoMod;

            // Scan and verify we see them all
            scanCursorFuncs.Initialize(verifyKeys);
            ClassicAssert.IsFalse(session.ScanCursor(ref cursor, long.MaxValue, scanCursorFuncs, long.MaxValue), "Expected scan to finish and return false, pt 1");
            ClassicAssert.AreEqual(TotalRecords, scanCursorFuncs.numRecords, "Unexpected count for all on-disk");
            ClassicAssert.AreEqual(0, cursor, "Expected cursor to be 0, pt 2");

            // Add another totalRecords, with keys incremented by totalRecords to remain distinct, and verify we see all keys.
            for (int i = 0; i < TotalRecords; i++)
            {
                var key1 = new KeyStruct { kfield1 = i + TotalRecords, kfield2 = i + TotalRecords + 1 };
                var value = new ValueStruct { vfield1 = i + TotalRecords, vfield2 = i + TotalRecords + 1 };
                _ = bContext.Upsert(ref key1, ref value);
            }
            scanCursorFuncs.Initialize(verifyKeys);
            ClassicAssert.IsFalse(session.ScanCursor(ref cursor, long.MaxValue, scanCursorFuncs, long.MaxValue), "Expected scan to finish and return false, pt 1");
            ClassicAssert.AreEqual(TotalRecords * 2, scanCursorFuncs.numRecords, "Unexpected count for on-disk + in-mem");
            ClassicAssert.AreEqual(0, cursor, "Expected cursor to be 0, pt 3");

            // Try an invalid cursor (not a multiple of 8) on-disk and verify we get one correct record. Use 3x page size to make sure page boundaries are tested.
            ClassicAssert.Greater(store.hlogBase.GetTailAddress(), PageSize * 10, "Need enough space to exercise this");
            scanCursorFuncs.Initialize(verifyKeys);
            cursor = store.hlogBase.BeginAddress - 1;
            do
            {
                ClassicAssert.IsTrue(session.ScanCursor(ref cursor, 1, scanCursorFuncs, long.MaxValue, validateCursor: true), "Expected scan to finish and return false, pt 1");
                cursor = scanCursorFuncs.lastAddress + recordSize + 1;
            } while (cursor < PageSize * 3);

            // Now try an invalid cursor in-memory. First we have to read what's at the target start address (let's use HeadAddress) to find what the value is.
            InputStruct input = default;
            OutputStruct output = default;
            ReadOptions readOptions = default;
            var readStatus = bContext.ReadAtAddress(store.hlogBase.HeadAddress, ref input, ref output, ref readOptions, out _);
            ClassicAssert.IsTrue(readStatus.Found, $"Could not read at HeadAddress; {readStatus}");

            scanCursorFuncs.Initialize(verifyKeys);
            scanCursorFuncs.numRecords = (int)output.value.vfield1;
            cursor = store.Log.HeadAddress + 1;
            do
            {
                ClassicAssert.IsTrue(session.ScanCursor(ref cursor, 1, scanCursorFuncs, long.MaxValue, validateCursor: true), "Expected scan to finish and return false, pt 1");
                cursor = scanCursorFuncs.lastAddress + recordSize + 1;
            } while (cursor < store.hlogBase.HeadAddress + PageSize * 3);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]

        public void BlittableScanCursorFilterTest([Values(HashModulo.NoMod, HashModulo.Hundred)] HashModulo hashMod)
        {
            var recordSize = BlittableAllocatorImpl<KeyStruct, ValueStruct, StructStoreFunctions>.RecordSize;

            using var session = store.NewSession<InputStruct, OutputStruct, Empty, ScanFunctions>(new ScanFunctions());
            var bContext = session.BasicContext;

            for (int i = 0; i < TotalRecords; i++)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                _ = bContext.Upsert(ref key1, ref value);
            }

            var scanCursorFuncs = new ScanCursorFuncs();

            long cursor = 0;
            scanCursorFuncs.Initialize(verifyKeys: false, k => k.kfield1 % 10 == 0);
            ClassicAssert.IsTrue(session.ScanCursor(ref cursor, 10, scanCursorFuncs, store.Log.TailAddress), "ScanCursor failed, pt 1");
            ClassicAssert.AreEqual(10, scanCursorFuncs.numRecords, "count at first 10");
            ClassicAssert.Greater(cursor, 0, "Expected cursor to be > 0, pt 1");

            // Now fake out the key verification to make it think we got all the previous keys; this ensures we are aligned as expected.
            scanCursorFuncs.Initialize(verifyKeys: true, k => true);
            scanCursorFuncs.numRecords = 91;   // (filter accepts: 0-9) * 10 + 1
            ClassicAssert.IsTrue(session.ScanCursor(ref cursor, 100, scanCursorFuncs, store.Log.TailAddress), "ScanCursor failed, pt 2");
            ClassicAssert.AreEqual(191, scanCursorFuncs.numRecords, "count at second 100");
            ClassicAssert.Greater(cursor, 0, "Expected cursor to be > 0, pt 1");
        }

        internal sealed class ScanCursorFuncs : IScanIteratorFunctions<KeyStruct, ValueStruct>
        {
            internal int numRecords;
            internal long lastAddress;
            internal bool verifyKeys;
            internal Func<KeyStruct, bool> filter;

            internal void Initialize(bool verifyKeys) => Initialize(verifyKeys, k => true);

            internal void Initialize(bool verifyKeys, Func<KeyStruct, bool> filter)
            {
                numRecords = 0;
                this.verifyKeys = verifyKeys;
                this.filter = filter;
            }

            public bool ConcurrentReader(ref KeyStruct key, ref ValueStruct value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
            {
                cursorRecordResult = filter(key) ? CursorRecordResult.Accept : CursorRecordResult.Skip;
                if (cursorRecordResult != CursorRecordResult.Accept)
                    return true;

                if (verifyKeys)
                    ClassicAssert.AreEqual(numRecords, key.kfield1, "Mismatched key field on Scan");
                ClassicAssert.Greater(recordMetadata.Address, 0);
                ++numRecords;
                lastAddress = recordMetadata.Address;
                return true;
            }

            public void OnException(Exception exception, long numberOfRecords)
                => Assert.Fail($"Unexpected exception at {numberOfRecords} records: {exception.Message}");

            public bool OnStart(long beginAddress, long endAddress) => true;

            public void OnStop(bool completed, long numberOfRecords) { }

            public bool SingleReader(ref KeyStruct key, ref ValueStruct value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                => ConcurrentReader(ref key, ref value, recordMetadata, numberOfRecords, out cursorRecordResult);
        }

        class LogObserver : IObserver<ITsavoriteScanIterator<KeyStruct, ValueStruct>>
        {
            int val = 0;

            public void OnCompleted()
            {
                ClassicAssert.AreEqual(TotalRecords, val);
            }

            public void OnError(Exception error)
            {
            }

            public void OnNext(ITsavoriteScanIterator<KeyStruct, ValueStruct> iter)
            {
                while (iter.GetNext(out _, out KeyStruct key, out ValueStruct value))
                {
                    ClassicAssert.AreEqual(val, key.kfield1);
                    ClassicAssert.AreEqual(val + 1, key.kfield2);
                    ClassicAssert.AreEqual(val, value.vfield1);
                    ClassicAssert.AreEqual(val + 1, value.vfield2);
                    val++;
                }
            }
        }
    }
}