// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test
{
    using ClassAllocator = ObjectAllocator<StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>>;
    using ClassStoreFunctions = StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>;
    [TestFixture]
    internal class ObjectIterationTests : TestBase
    {
        private TsavoriteKV<ClassStoreFunctions, ClassAllocator> store;
        private ClientSession<TestObjectKey, TestObjectInput, TestObjectOutput, int, TestObjectFunctionsDelete, ClassStoreFunctions, ClassAllocator> session;
        private BasicContext<TestObjectKey, TestObjectInput, TestObjectOutput, int, TestObjectFunctionsDelete, ClassStoreFunctions, ClassAllocator> bContext;
        private IDevice log, objlog;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            // Tests call InternalSetup()
        }

        private void InternalSetup(bool largeMemory)
        {
            // Broke this out as we have different requirements by test.
            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "ObjectIterationTests.log"), deleteOnClose: true);
            objlog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "ObjectIterationTests.obj.log"), deleteOnClose: true);

            store = new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                ObjectLogDevice = objlog,
                MutableFraction = 0.1,
                LogMemorySize = 1L << (largeMemory ? 25 : 14),
                PageSize = 1L << (largeMemory ? 20 : 9)
            }, StoreFunctions.Create(new TestObjectKey.Comparer(), () => new TestObjectValue.Serializer(), DefaultRecordTriggers.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );
            session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, int, TestObjectFunctionsDelete>(new TestObjectFunctionsDelete());
            bContext = session.BasicContext;
        }

        [TearDown]
        public void TearDown()
        {
            session?.Dispose();
            session = null;
            store?.Dispose();
            store = null;
            log?.Dispose();
            log = null;
            objlog?.Dispose();
            objlog = null;

            OnTearDown();
        }

        internal struct ObjectPushIterationTestFunctions : IScanIteratorFunctions
        {
            internal int keyMultToValue;
            internal long numRecords;
            internal int stopAt;

            public bool Reader<TSourceLogRecord>(in TSourceLogRecord logRecord, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                where TSourceLogRecord : ISourceLogRecord
            {
                cursorRecordResult = CursorRecordResult.Accept; // default; not used here
                if (keyMultToValue > 0)
                    ClassicAssert.AreEqual(logRecord.Key.AsRef<TestObjectKey>().key * keyMultToValue, ((TestObjectValue)logRecord.ValueObject).value);
                return stopAt != ++numRecords;
            }

            public readonly bool OnStart(long beginAddress, long endAddress) => true;
            public readonly void OnException(Exception exception, long numberOfRecords) { }
            public readonly void OnStop(bool completed, long numberOfRecords) { }
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        [Category(SmokeTestCategory)]

        public void ObjectIterationBasicTest([Values] ScanIteratorType scanIteratorType)
        {
            InternalSetup(largeMemory: false);
            ObjectPushIterationTestFunctions scanIteratorFunctions = new();

            const int totalRecords = 2000;

            void iterateAndVerify(int keyMultToValue, int expectedRecs)
            {
                scanIteratorFunctions.keyMultToValue = keyMultToValue;
                scanIteratorFunctions.numRecords = 0;

                if (scanIteratorType == ScanIteratorType.Pull)
                {
                    using var iter = session.Iterate();
                    while (iter.GetNext())
                        _ = scanIteratorFunctions.Reader(in iter, default, default, out _);
                }
                else
                    ClassicAssert.IsTrue(session.Iterate(ref scanIteratorFunctions), $"Failed to complete push iteration; numRecords = {scanIteratorFunctions.numRecords}");

                ClassicAssert.AreEqual(expectedRecs, scanIteratorFunctions.numRecords);
            }

            // Initial population
            for (int i = 0; i < totalRecords; i++)
            {
                var key1 = new TestObjectKey { key = i };
                var value = new TestObjectValue { value = i };
                _ = bContext.Upsert(key1, value);
            }
            iterateAndVerify(1, totalRecords);

            for (int i = 0; i < totalRecords; i++)
            {
                var key1 = new TestObjectKey { key = i };
                var value = new TestObjectValue { value = 2 * i };
                _ = bContext.Upsert(key1, value);
            }
            iterateAndVerify(2, totalRecords);

            for (int i = totalRecords / 2; i < totalRecords; i++)
            {
                var key1 = new TestObjectKey { key = i };
                var value = new TestObjectValue { value = i };
                _ = bContext.Upsert(key1, value);
            }
            iterateAndVerify(0, totalRecords);

            for (int i = 0; i < totalRecords; i += 2)
            {
                var key1 = new TestObjectKey { key = i };
                var value = new TestObjectValue { value = i };
                _ = bContext.Upsert(key1, value);
            }
            iterateAndVerify(0, totalRecords);

            for (int i = 0; i < totalRecords; i += 2)
            {
                var key1 = new TestObjectKey { key = i };
                _ = bContext.Delete(key1);
            }
            iterateAndVerify(0, totalRecords / 2);

            for (int i = 0; i < totalRecords; i++)
            {
                var key1 = new TestObjectKey { key = i };
                var value = new TestObjectValue { value = 3 * i };
                _ = bContext.Upsert(key1, value);
            }
            iterateAndVerify(3, totalRecords);

            store.Log.FlushAndEvict(wait: true);
            iterateAndVerify(3, totalRecords);
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        [Category(SmokeTestCategory)]

        public void ObjectIterationPushStopTest()
        {
            InternalSetup(largeMemory: false);
            ObjectPushIterationTestFunctions scanIteratorFunctions = new();

            const int totalRecords = 2000;
            var start = store.Log.TailAddress;

            void scanAndVerify(int stopAt, bool useScan)
            {
                scanIteratorFunctions.numRecords = 0;
                scanIteratorFunctions.stopAt = stopAt;
                if (useScan)
                    ClassicAssert.IsFalse(store.Log.Scan(ref scanIteratorFunctions, start, store.Log.TailAddress), $"Failed to terminate push iteration early; numRecords = {scanIteratorFunctions.numRecords}");
                else
                    ClassicAssert.IsFalse(session.Iterate(ref scanIteratorFunctions), $"Failed to terminate push iteration early; numRecords = {scanIteratorFunctions.numRecords}");
                ClassicAssert.AreEqual(stopAt, scanIteratorFunctions.numRecords);
            }

            // Initial population
            for (int i = 0; i < totalRecords; i++)
            {
                var key1 = new TestObjectKey { key = i };
                var value = new TestObjectValue { value = i };
                _ = bContext.Upsert(key1, value);
            }

            scanAndVerify(42, useScan: true);
            scanAndVerify(42, useScan: false);
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        [Category(SmokeTestCategory)]
        //[Repeat(3000)]
        //[Explicit("Temporary: accessing a disposed object")]
        public void ObjectIterationPushLockTest([Values(1, 2, 4, 8)] int scanThreads, [Values(0, 1, 4)] int updateThreads, [Values] ScanMode scanMode, [Values] bool largeMemory)
        {
            InternalSetup(largeMemory);

            const int totalRecords = 2000;
            var start = store.Log.TailAddress;

            void LocalScan(int i)
            {
                using var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, int, TestObjectFunctionsDelete>(new TestObjectFunctionsDelete());
                ObjectPushIterationTestFunctions scanIteratorFunctions = new();

                var end = store.Log.TailAddress;
                if (scanMode == ScanMode.Scan)
                    Assert.That(store.Log.Scan(ref scanIteratorFunctions, start, end), Is.True, $"Failed to complete push scan; numRecords = {scanIteratorFunctions.numRecords}, start = {start}, end = {end}");
                else
                    Assert.That(session.Iterate(ref scanIteratorFunctions), Is.True, $"Failed to complete push iteration; numRecords = {scanIteratorFunctions.numRecords}, start = {start}, end = {end}");

                // If we are doing Scan with updates and without largeMemory, there will be records appended at the log tail due to not 
                // being able to do IPU, so the scan count may be > totalRecords.
                if (scanMode == ScanMode.Scan && !largeMemory && updateThreads > 0)
                    Assert.That(scanIteratorFunctions.numRecords, Is.GreaterThanOrEqualTo(totalRecords));
                else
                    Assert.That(scanIteratorFunctions.numRecords, Is.EqualTo(totalRecords));
            }

            const int keyTag = 0x420000;

            void LocalUpdate(int tid)
            {
                using var localSession = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, int, TestObjectFunctionsDelete>(new TestObjectFunctionsDelete());
                var localBContext = localSession.BasicContext;
                for (int i = 0; i < totalRecords; i++)
                {
                    var key1 = new TestObjectKey { key = i + keyTag };
                    var value = new TestObjectValue { value = (tid + 1) * i };
                    var status = localBContext.Upsert(key1, value);
                    Assert.That(status.IsPending, Is.False, "Upsert should not go pending");
                }
            }

            { // Initial population
                for (int i = 0; i < totalRecords; i++)
                {
                    var key1 = new TestObjectKey { key = i + keyTag };
                    var value = new TestObjectValue { value = i + 340000 };
                    var status = bContext.Upsert(key1, value);
                    Assert.That(status.IsPending, Is.False, "Upsert should not go pending");
                }
            }

            List<Task> tasks = [];   // Task rather than Thread for propagation of exception.
            var numThreads = scanThreads + updateThreads;
            for (int t = 0; t < numThreads; t++)
            {
                var tid = t;
                if (t < scanThreads)
                    tasks.Add(Task.Factory.StartNew(() => LocalScan(tid)));
                else
                    tasks.Add(Task.Factory.StartNew(() => LocalUpdate(tid)));
            }
            Task.WaitAll([.. tasks]);
        }
    }
    [TestFixture]
    internal class ObjectIterationTests2 : TestBase
    {
        // Per issue #1630, handle 4 pages worth of records with an InsertAll-DeleteAll-ReInsertAll pattern.
        public class InsDelIns_ScanIteratorFunctions : IScanIteratorFunctions
        {
            public const int MaxCount = 64;
            public readonly (int Key, int Value)[] ExpectedItems = new (int Key, int Value)[MaxCount];
            public readonly List<(int Key, int Value)> UnexpectedItems = [];
            public int Count;

            public bool Reader<TSourceLogRecord>(in TSourceLogRecord sourceLogRecord, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                where TSourceLogRecord : ISourceLogRecord
            {
                ref readonly var key = ref sourceLogRecord.Key.AsRef<int>();
                ref readonly var value = ref sourceLogRecord.ValueSpan.AsRef<int>();
                if (Count < ExpectedItems.Length)
                    ExpectedItems[Count] = (key, value);
                else
                    UnexpectedItems.Add((key, value));

                Count++;
                if (Count == MaxCount)
                {
                    cursorRecordResult = CursorRecordResult.Accept | CursorRecordResult.EndBatch;
                    WriteLine($"EndBatch {key},{value}");
                }
                else
                {
                    // If this happens, it is likely because we had to go to pending IO and had some pending when we triggered the EndBatch at MaxCount;
                    // the pending operations are completed which call back to here.
                    cursorRecordResult = CursorRecordResult.Accept;
                    WriteLine($"Accept {key},{value}");
                }
                return true;
            }
            public bool OnStart(long beginAddress, long endAddress)
            {
                Count = 0;
                WriteLine("OnStart");
                return true;
            }
            public void OnException(Exception exception, long numberOfRecords) => WriteLine($"{exception.GetType().Name}: {exception.Message}; numRec {numberOfRecords}");
            public void OnStop(bool completed, long numberOfRecords) => WriteLine("OnStop");

            // For debugging only
            internal static void WriteLine(string s) { } // => Debug.WriteLine(s);
        }

        private const int Count = 249;  // In the Issue this is 253, but in V2 the presence of PageHeader uses some space and that causes 253 to require another page.
        private const long PageSize = 1L << 12;
        private const long SegmentSize = PageSize;

        private static void RunTest(IDevice LogDevice)
        {
            var Settings = new KVSettings
            {
                IndexSize = 1L << 6,
                LogMemorySize = 1L << 13,
                PageSize = PageSize,
                SegmentSize = SegmentSize,
                MutableFraction = 1,
                LogDevice = LogDevice,
            };
            var StoreFunctions = new StoreFunctions<SpanByteComparer, DefaultRecordTriggers>(new SpanByteComparer(), () => null, new DefaultRecordTriggers());
            using var Store = new TsavoriteKV<StoreFunctions<SpanByteComparer, DefaultRecordTriggers>, ObjectAllocator<StoreFunctions<SpanByteComparer, DefaultRecordTriggers>>>(
                Settings, StoreFunctions,
                static (AllocSettings, StoreFuncs) => new ObjectAllocator<StoreFunctions<SpanByteComparer, DefaultRecordTriggers>>(AllocSettings, StoreFuncs));
            using var ReadAddSession = Store.NewSession<TestSpanByteKey, PinnedSpanByte, SpanByteAndMemory, Empty, SpanByteFunctions<Empty>>(new SpanByteFunctions<Empty>(System.Buffers.MemoryPool<byte>.Shared));

            Span<int> keySpan = stackalloc int[1];
            Span<int> valueSpan = stackalloc int[1];
            var key = TestSpanByteKey.FromPinnedSpan(MemoryMarshal.AsBytes(keySpan));
            var value = MemoryMarshal.AsBytes(valueSpan);

            // Insert all
            for (var Key = 0; Key < Count; Key++)
            {
                keySpan[0] = Key;
                valueSpan[0] = Key;
                _ = ReadAddSession.BasicContext.Upsert(key, value);
            }

            // Delete all
            for (var Key = 0; Key < Count; Key++)
            {
                keySpan[0] = Key;
                var Status = ReadAddSession.BasicContext.Delete(key, default);
            }

            // ReInsert all
            for (var Key = 0; Key < Count; Key++)
            {
                keySpan[0] = Key;
                valueSpan[0] = Key;
                _ = ReadAddSession.BasicContext.Upsert(key, value);
            }

            var ScanIteratorFunctions = new InsDelIns_ScanIteratorFunctions();
            long TotalCount = 0, Cursor = 0;
            while (true)
            {
                _ = ReadAddSession.IterateLookup(ref ScanIteratorFunctions, ref Cursor, resetCursor: false);
                TotalCount += ScanIteratorFunctions.Count;
                InsDelIns_ScanIteratorFunctions.WriteLine($"while: {TotalCount}");

                // If we did not get a full batch, we're done
                if (ScanIteratorFunctions.Count < InsDelIns_ScanIteratorFunctions.MaxCount)
                    break;
            }
            Assert.That(TotalCount, Is.EqualTo(Count));
            Assert.That(ScanIteratorFunctions.UnexpectedItems, Is.Empty, "Unexpected items were sent to Reader(), probably pending items that were in-flight when EndBatch was received");
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        [Category(SmokeTestCategory)]
        public void InsDelIns_LocalMemory()
        {
            using var LogDevice = new LocalMemoryDevice(capacity: SegmentSize, sz_segment: SegmentSize, parallelism: 1, sector_size: 64U, latencyMs: 0, fileName: Path.Combine(MethodTestDir, "test.log"));
            RunTest(LogDevice);
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        [Category(SmokeTestCategory)]
        public void InsDelIns_MLSD()
        {
            using var LogDevice = new ManagedLocalStorageDevice(filename: Path.Combine(MethodTestDir, "test.log"),
                capacity: SegmentSize,
                deleteOnClose: true
            );
            RunTest(LogDevice);
        }
    }
}