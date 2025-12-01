// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#if LOGRECORD_TODO

using System;
using System.IO;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test
{
    // Must be in a separate block so the "using ClassStoreFunctions" is the first line in its namespace declaration.
    public class MyObjectComparerModulo : IKeyComparer<MyKey>
    {
        readonly long mod;

        internal MyObjectComparerModulo(long mod) => this.mod = mod;

        public bool Equals(ref MyKey k1, ref MyKey k2) => k1.key == k2.key;

        // Force collisions to create a chain
        public long GetHashCode64(ref MyKey key)
        {
            long hash = Utility.GetHashCode(key.key);
            return mod > 0 ? hash % mod : hash;
        }
    }
}

namespace Tsavorite.test
{
    using ClassAllocator = GenericAllocator<MyKey, MyValue, StoreFunctions<MyKey, MyValue, MyObjectComparerModulo, DefaultRecordDisposer<MyKey, MyValue>>>;
    using ClassStoreFunctions = StoreFunctions<MyKey, MyValue, MyObjectComparerModulo, DefaultRecordDisposer<MyKey, MyValue>>;

    [TestFixture]
    internal class GenericLogScanTests
    {
        private TsavoriteKV<MyKey, MyValue, ClassStoreFunctions, ClassAllocator> store;
        private IDevice log, objlog;
        const int TotalRecords = 250;

        MyObjectComparerModulo comparer;

        [SetUp]
        public void Setup()
        {
            // Clean up log files from previous test runs in case they weren't cleaned up
            DeleteDirectory(MethodTestDir, wait: true);

            comparer = null;
            foreach (var arg in TestContext.CurrentContext.Test.Arguments)
            {
                if (arg is HashModulo mod && mod == HashModulo.Hundred)
                {
                    comparer = new MyObjectComparerModulo(100);
                    continue;
                }
            }
            comparer ??= new(0);
        }

        [TearDown]
        public void TearDown()
        {
            store?.Dispose();
            store = null;
            log?.Dispose();
            log = null;
            objlog?.Dispose();
            objlog = null;

            DeleteDirectory(MethodTestDir);
        }

        internal struct GenericPushScanTestFunctions : IScanIteratorFunctions<MyKey, MyValue>
        {
            internal long numRecords;

            public readonly bool OnStart(long beginAddress, long endAddress) => true;

            public bool Reader(ref MyKey key, ref MyValue value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
            {
                cursorRecordResult = CursorRecordResult.Accept; // default; not used here
                ClassicAssert.AreEqual(numRecords, key.key, $"log scan 1: key");
                ClassicAssert.AreEqual(numRecords, value.value, $"log scan 1: value");

                ++numRecords;
                return true;
            }

            public readonly void OnException(Exception exception, long numberOfRecords) { }

            public readonly void OnStop(bool completed, long numberOfRecords) { }
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void DiskWriteScanBasicTest([Values] TestDeviceType deviceType, [Values] ScanIteratorType scanIteratorType)
        {
            log = CreateTestDevice(deviceType, Path.Join(MethodTestDir, $"DiskWriteScanBasicTest_{deviceType}.log"));
            objlog = CreateTestDevice(deviceType, Path.Join(MethodTestDir, $"DiskWriteScanBasicTest_{deviceType}.obj.log"));
            store = new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                ObjectLogDevice = objlog,
                MutableFraction = 0.1,
                MemorySize = 1L << 15,
                PageSize = 1L << 9,
                SegmentSize = 1L << 22
            }, StoreFunctions<MyKey, MyValue>.Create(comparer, () => new MyKeySerializer(), () => new MyValueSerializer())
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            using var session = store.NewSession<MyInput, MyOutput, Empty, MyFunctions>(new MyFunctions());
            var bContext = session.BasicContext;

            using var s = store.Log.Subscribe(new LogObserver());

            var start = store.Log.TailAddress;
            for (int i = 0; i < TotalRecords; i++)
            {
                var _key = new MyKey { key = i };
                var _value = new MyValue { value = i };
                _ = bContext.Upsert(ref _key, ref _value, Empty.Default);
                if (i % 100 == 0)
                    store.Log.FlushAndEvict(true);
            }
            store.Log.FlushAndEvict(true);

            GenericPushScanTestFunctions scanIteratorFunctions = new();

            void scanAndVerify(ScanBufferingMode sbm)
            {
                scanIteratorFunctions.numRecords = 0;

                if (scanIteratorType == ScanIteratorType.Pull)
                {
                    using var iter = store.Log.Scan(start, store.Log.TailAddress, sbm);
                    while (iter.GetNext(out var recordInfo))
                        _ = scanIteratorFunctions.Reader(ref iter.GetKey(), ref iter.GetValue(), default, default, out _);
                }
                else
                    ClassicAssert.IsTrue(store.Log.Scan(ref scanIteratorFunctions, start, store.Log.TailAddress, sbm), "Failed to complete push iteration");

                ClassicAssert.AreEqual(TotalRecords, scanIteratorFunctions.numRecords);
            }

            scanAndVerify(ScanBufferingMode.SinglePageBuffering);
            scanAndVerify(ScanBufferingMode.DoublePageBuffering);
        }

        class LogObserver : IObserver<ITsavoriteScanIterator<MyKey, MyValue>>
        {
            int val = 0;

            public void OnCompleted()
            {
                ClassicAssert.AreEqual(val == TotalRecords, $"LogObserver.OnCompleted: totalRecords");
            }

            public void OnError(Exception error)
            {
            }

            public void OnNext(ITsavoriteScanIterator<MyKey, MyValue> iter)
            {
                while (iter.GetNext(out _, out MyKey key, out MyValue value))
                {
                    ClassicAssert.AreEqual(val, key.key, $"LogObserver.OnNext: key");
                    ClassicAssert.AreEqual(val, value.value, $"LogObserver.OnNext: value");
                    val++;
                }
            }
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]

        public void BlittableScanJumpToBeginAddressTest()
        {
            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "test.log"));
            objlog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "test.obj.log"));
            store = new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                ObjectLogDevice = objlog,
                MutableFraction = 0.1,
                MemorySize = 1L << 20,
                PageSize = 1L << 15,
                SegmentSize = 1L << 18
            }, StoreFunctions<MyKey, MyValue>.Create(comparer, () => new MyKeySerializer(), () => new MyValueSerializer())
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            using var session = store.NewSession<MyInput, MyOutput, Empty, MyFunctions>(new MyFunctions());
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
                var key = new MyKey { key = i };
                var value = new MyValue { value = i };
                _ = bContext.Upsert(ref key, ref value, Empty.Default);
            }

            using var iter = store.Log.Scan(store.Log.HeadAddress, store.Log.TailAddress);

            for (int i = 0; i < 100; ++i)
            {
                ClassicAssert.IsTrue(iter.GetNext(out var recordInfo));
                ClassicAssert.AreEqual(i, iter.GetKey().key);
                ClassicAssert.AreEqual(i, iter.GetValue().value);
            }

            store.Log.ShiftBeginAddress(shiftBeginAddressTo);

            for (int i = 0; i < numTailRecords; ++i)
            {
                ClassicAssert.IsTrue(iter.GetNext(out var recordInfo));
                if (i == 0)
                    ClassicAssert.AreEqual(store.Log.BeginAddress, iter.CurrentAddress);
                var expectedKey = numRecords - numTailRecords + i;
                ClassicAssert.AreEqual(expectedKey, iter.GetKey().key);
                ClassicAssert.AreEqual(expectedKey, iter.GetValue().value);
            }
        }

        public class ScanFunctions : MyFunctions
        {
            // Right now this is unused but helped with debugging so I'm keeping it around.
            internal long insertedAddress;

            public override bool InitialWriter(ref MyKey key, ref MyInput input, ref MyValue src, ref MyValue dst, ref MyOutput output, ref UpsertInfo upsertInfo, ref RecordInfo recordInfo)
            {
                insertedAddress = upsertInfo.Address;
                return base.InitialWriter(ref key, ref input, ref src, ref dst, ref output, ref upsertInfo, reason, ref recordInfo);
            }
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]

        public void GenericScanCursorTest([Values(HashModulo.NoMod, HashModulo.Hundred)] HashModulo hashMod)
        {
            const int PageSizeBits = 9;
            const long PageSize = 1L << PageSizeBits;
            var recordSize = GenericAllocatorImpl<MyKey, MyValue, ClassStoreFunctions>.RecordSize;

            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "test.log"));
            objlog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "test.obj.log"));

            store = new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                ObjectLogDevice = objlog,
                MutableFraction = 0.1,
                MemorySize = 1L << 20,
                PageSize = 1L << 15,
                SegmentSize = 1L << 18
            }, StoreFunctions<MyKey, MyValue>.Create(comparer, () => new MyKeySerializer(), () => new MyValueSerializer())
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );


            using var session = store.NewSession<MyInput, MyOutput, Empty, ScanFunctions>(new ScanFunctions());
            var bContext = session.BasicContext;

            for (int i = 0; i < TotalRecords; i++)
            {
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };
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
                var key1 = new MyKey { key = i + TotalRecords };
                var value = new MyValue { value = i + TotalRecords };
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
            MyInput input = new();
            MyOutput output = new();
            ReadOptions readOptions = default;
            var readStatus = bContext.ReadAtAddress(store.hlogBase.HeadAddress, ref input, ref output, ref readOptions, out _);
            ClassicAssert.IsTrue(readStatus.Found, $"Could not read at HeadAddress; {readStatus}");

            scanCursorFuncs.Initialize(verifyKeys);
            scanCursorFuncs.numRecords = output.value.value;
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

        public void GenericScanCursorFilterTest([Values(HashModulo.NoMod, HashModulo.Hundred)] HashModulo hashMod)
        {
            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "test.log"));
            objlog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "test.obj.log"));

            store = new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                ObjectLogDevice = objlog,
                MutableFraction = 0.1,
                MemorySize = 1L << 20,
                PageSize = 1L << 15,
                SegmentSize = 1L << 18
            }, StoreFunctions<MyKey, MyValue>.Create(comparer, () => new MyKeySerializer(), () => new MyValueSerializer())
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            using var session = store.NewSession<MyInput, MyOutput, Empty, ScanFunctions>(new ScanFunctions());
            var bContext = session.BasicContext;

            for (int i = 0; i < TotalRecords; i++)
            {
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };
                _ = bContext.Upsert(ref key1, ref value);
            }

            var scanCursorFuncs = new ScanCursorFuncs();

            long cursor = 0;
            scanCursorFuncs.Initialize(verifyKeys: false, k => k.key % 10 == 0);
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

        internal sealed class ScanCursorFuncs : IScanIteratorFunctions<MyKey, MyValue>
        {
            internal int numRecords;
            internal long lastAddress;
            internal bool verifyKeys;
            internal Func<MyKey, bool> filter;

            internal void Initialize(bool verifyKeys) => Initialize(verifyKeys, k => true);

            internal void Initialize(bool verifyKeys, Func<MyKey, bool> filter)
            {
                numRecords = 0;
                this.verifyKeys = verifyKeys;
                this.filter = filter;
            }

            public bool Reader(ref MyKey key, ref MyValue value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
            {
                cursorRecordResult = filter(key) ? CursorRecordResult.Accept : CursorRecordResult.Skip;
                if (cursorRecordResult != CursorRecordResult.Accept)
                    return true;

                if (verifyKeys)
                    ClassicAssert.AreEqual(numRecords, key.key, "Mismatched key field on Scan");
                ClassicAssert.Greater(recordMetadata.Address, 0);
                ++numRecords;
                lastAddress = recordMetadata.Address;
                return true;
            }

            public void OnException(Exception exception, long numberOfRecords)
                => Assert.Fail($"Unexpected exception at {numberOfRecords} records: {exception.Message}");

            public bool OnStart(long beginAddress, long endAddress) => true;

            public void OnStop(bool completed, long numberOfRecords) { }
        }
    }
}

#endif // LOGRECORD_TODO