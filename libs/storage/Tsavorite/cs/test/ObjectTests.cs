// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Allure.NUnit;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.core.Utility;

namespace Tsavorite.test.Objects
{
    using static TestUtils;

    using ClassAllocator = ObjectAllocator<StoreFunctions<TestObjectKey.Comparer, DefaultRecordDisposer>>;
    using ClassStoreFunctions = StoreFunctions<TestObjectKey.Comparer, DefaultRecordDisposer>;

    [AllureNUnit]
    [TestFixture]
    internal class ObjectTests : AllureTestBase
    {
        private TsavoriteKV<ClassStoreFunctions, ClassAllocator> store;
        private IDevice log, objlog;
        const long LogMemorySize = 1L << 15;
        const long PageSize = 1L << 10;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "ObjectTests.log"), deleteOnClose: true);
            objlog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "ObjectTests.obj.log"), deleteOnClose: true);
            var storeFunctions = TestContext.CurrentContext.Test.MethodName.StartsWith("LargeObject")
                ? StoreFunctions.Create(new TestObjectKey.Comparer(), () => new TestLargeObjectValue.Serializer(), DefaultRecordDisposer.Instance)
                : StoreFunctions.Create(new TestObjectKey.Comparer(), () => new TestObjectValue.Serializer(), DefaultRecordDisposer.Instance);

            store = new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                ObjectLogDevice = objlog,
                MutableFraction = TestContext.CurrentContext.Test.MethodName.Equals(nameof(LargeObjectLinearizeFlushedPages)) ? 1.0 : 0.1,
                LogMemorySize = LogMemorySize,
                PageSize = PageSize
            }, storeFunctions
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
            objlog?.Dispose();
            objlog = null;
            OnTearDown();
        }

        [Test, Category(TsavoriteKVTestCategory), Category(SmokeTestCategory), Category(ObjectIdMapCategory)]
        public void ObjectInMemWriteReadUpsert()
        {
            using var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions());
            var bContext = session.BasicContext;

            TestObjectKey key = new() { key = 9999999 };
            TestObjectValue value = new() { value = 23 };

            TestObjectInput input = default;
            TestObjectOutput output = default;

            _ = bContext.Upsert(key, value, Empty.Default);
            _ = bContext.Read(key, ref input, ref output, Empty.Default);
            ClassicAssert.AreEqual(value.value, output.value.value);
        }

        [Test, Category(TsavoriteKVTestCategory), Category(SmokeTestCategory), Category(ObjectIdMapCategory)]
        public void ObjectInMemWriteReadRMW()
        {
            using var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions());
            var bContext = session.BasicContext;

            TestObjectKey key1 = new() { key = 8999998 };
            TestObjectInput input1 = new() { value = 23 };
            TestObjectOutput output = new();

            _ = bContext.RMW(key1, ref input1, Empty.Default);

            TestObjectKey key2 = new() { key = 8999999 };
            TestObjectInput input2 = new() { value = 24 };
            _ = bContext.RMW(key2, ref input2, Empty.Default);

            _ = bContext.Read(key1, ref input1, ref output, Empty.Default);

            ClassicAssert.AreEqual(input1.value, output.value.value);

            _ = bContext.Read(key2, ref input2, ref output, Empty.Default);
            ClassicAssert.AreEqual(input2.value, output.value.value);
        }

        [Test, Category(TsavoriteKVTestCategory), Category(LogRecordCategory), Category(SmokeTestCategory), Category(ObjectIdMapCategory)]
        public void ObjectDiskWriteReadSingle()
        {
            using var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions());
            var bContext = session.BasicContext;
            const int keyInt = 42;

            var key = new TestObjectKey { key = keyInt };
            var value = new TestObjectValue { value = keyInt };
            _ = bContext.Upsert(key, value, Empty.Default);

            TestObjectInput input = new();
            TestObjectOutput output = new();
            var status = bContext.Read(key, ref input, ref output, Empty.Default);
            Assert.That(status.IsPending, Is.False);
            Assert.That(status.Found, Is.True);
            Assert.That(output.value.value, Is.EqualTo(keyInt));

            store.Log.FlushAndEvict(wait: true);

            status = bContext.Read(key, ref input, ref output, Empty.Default);
            Assert.That(status.IsPending, Is.True);
            Assert.That(bContext.CompletePendingWithOutputs(out var outputs, wait: true), Is.True);
            (status, output) = GetSinglePendingResult(outputs);
            Assert.That(status.Found, Is.True);
            Assert.That(output.value.value, Is.EqualTo(keyInt));
        }

        [Test, Category(TsavoriteKVTestCategory), Category(LogRecordCategory), Category(SmokeTestCategory), Category(ObjectIdMapCategory)]
        public void ObjectDiskWriteRead()
        {
            using var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions());
            var bContext = session.BasicContext;

            for (int i = 0; i < 2000; i++)
            {
                var key = new TestObjectKey { key = i };
                var value = new TestObjectValue { value = i };
                if (i == 120)
                    i += 0;
                _ = bContext.Upsert(key, value, Empty.Default);
            }

            TestObjectKey key2 = new() { key = 23 };
            TestObjectInput input = new();
            TestObjectOutput g1 = new();
            var status = bContext.Read(key2, ref input, ref g1, Empty.Default);

            if (status.IsPending)
            {
                _ = bContext.CompletePendingWithOutputs(out var outputs, wait: true);
                (status, g1) = GetSinglePendingResult(outputs);
            }

            ClassicAssert.IsTrue(status.Found);
            ClassicAssert.AreEqual(23, g1.value.value);

            key2.key = 99999;
            status = bContext.Read(key2, ref input, ref g1, Empty.Default);

            if (status.IsPending)
                (status, _) = bContext.GetSinglePendingResult();
            ClassicAssert.IsFalse(status.Found);

            // Update last 100 using RMW in memory
            for (int i = 1900; i < 2000; i++)
            {
                var key = new TestObjectKey { key = i };
                input = new TestObjectInput { value = 1 };
                status = bContext.RMW(key, ref input, Empty.Default);
                ClassicAssert.IsFalse(status.IsPending, "Expected RMW to complete in-memory");
            }

            // Update first 100 using RMW from storage
            var numPendingUpdates = 0;
            for (int i = 0; i < 100; i++)
            {
                var key = new TestObjectKey { key = i };
                input = new TestObjectInput { value = 1 };
                status = bContext.RMW(key, ref input, Empty.Default);
                if (status.IsPending)
                {
                    numPendingUpdates++;
                    _ = bContext.CompletePending(true);
                }
            }
            Assert.That(numPendingUpdates, Is.EqualTo(100));

            var numPendingReads = 0;
            for (int i = 0; i < 2000; i++)
            {
                var output = new TestObjectOutput();
                var key = new TestObjectKey { key = i };
                var value = new TestObjectValue { value = i };

                status = bContext.Read(key, ref input, ref output, Empty.Default);
                if (status.IsPending)
                {
                    numPendingReads++;
                    (status, output) = bContext.GetSinglePendingResult();
                }

                if (i is < 100 or >= 1900)
                    ClassicAssert.AreEqual(value.value + 1, output.value.value);
                else
                    ClassicAssert.AreEqual(value.value, output.value.value);
            }
            Assert.That(numPendingReads, Is.GreaterThanOrEqualTo(numPendingUpdates));
        }

        /// <summary>Various sizes to test</summary>
        public enum SerializeKeyValueSize
        {
            Thirty = 30,
            OneK = 1024,
            HalfBuffer = IStreamBuffer.BufferSize / 2,
            OneBuffer = IStreamBuffer.BufferSize,
            ThreeHalfBuffer = (IStreamBuffer.BufferSize / 2) * 3,
            TwoBuffer = IStreamBuffer.BufferSize * 2
        }

        [Test, Category(TsavoriteKVTestCategory), Category(LogRecordCategory), Category(SmokeTestCategory), Category(ObjectIdMapCategory)]
        //[Repeat(300)]
        public void LargeObjectDiskWriteReadSmallKeyBigValue([Values] SerializeKeyValueSize serializeValueSize)
        {
            using var session = store.NewSession<TestObjectKey, TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions>(new TestLargeObjectFunctions());
            var bContext = session.BasicContext;

            var input = new TestLargeObjectInput();
            var output = new TestLargeObjectOutput();
            var valueSize = (int)serializeValueSize;
            const int numRec = 3;
            for (int ii = 0; ii < numRec; ii++)
            {
                var key = new TestObjectKey { key = ii };
                var value = new TestLargeObjectValue(valueSize + (ii * 4096));
                new Span<byte>(value.value).Fill(0x42);
                _ = bContext.Upsert(key, ref input, value, ref output);
            }

            // Test before and after the flush
            DoRead(onDisk: false);
            store.Log.FlushAndEvict(wait: true);
            DoRead(onDisk: true);

            void DoRead(bool onDisk)
            {
                TestLargeObjectInput input = new() { wantValueStyle = TestValueStyle.Object };
                for (int ii = 0; ii < numRec; ii++)
                {
                    var output = new TestLargeObjectOutput();
                    var key = new TestObjectKey { key = ii };

                    var status = bContext.Read(key, ref input, ref output, Empty.Default);
                    Assert.That(status.IsPending, Is.EqualTo(onDisk), $"IsPending ({status.IsPending}) != onDisk");
                    if (status.IsPending)
                        (status, output) = bContext.GetSinglePendingResult();

                    Assert.That(output.valueObject.value.Length, Is.EqualTo(valueSize + (ii * 4096)));
                    var numLongs = output.valueObject.value.Length % 8;
                    var badIndex = new ReadOnlySpan<byte>(output.valueObject.value).IndexOfAnyExcept((byte)0x42);
                    if (badIndex != -1)
                        Assert.Fail($"Unexpected byte value at index {badIndex}, onDisk {onDisk}, record# {ii}: {output.valueObject.value[badIndex]}");
                }
            }
        }

        [Test, Category(TsavoriteKVTestCategory), Category(LogRecordCategory), Category(SmokeTestCategory), Category(ObjectIdMapCategory)]
        //[Repeat(300)]
        [Explicit("Temporary while I figure out why FlushedUntilAddress is not as expected")]
        public void LargeObjectMultiFlushedPages([Values(SerializeKeyValueSize.Thirty, SerializeKeyValueSize.OneK)] SerializeKeyValueSize serializeValueSize)
        {
            // Ensure our size calculations are correct by validating the test parameters are what we expect
            Assert.That(LogMemorySize, Is.EqualTo(1L << 15));
            Assert.That(PageSize, Is.EqualTo(1L << 10));
            Assert.That(store.hlogBase.BufferSize, Is.EqualTo(32));     // LogMemorySize is PageSize << 5
            Assert.That(store.hlogBase.MaxAllocatedPageCount, Is.EqualTo(32));
            const int RecordLength = 32;                                // LogRecord allocated size
            const int ObjectsPerPage = (int)((PageSize - PageHeader.Size) / RecordLength);
            Assert.That(ObjectsPerPage, Is.EqualTo(30));                // Make debugging easier by verifying the length we'll see in the IDE

            var functions = new TestLargeObjectFunctions { expectedRecordLength = RecordLength }; // ExpectedRecordLength controls how many objects per page
            using var session = store.NewSession<TestObjectKey, TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions>(functions);
            var bContext = session.BasicContext;

            var input = new TestLargeObjectInput();
            var output = new TestLargeObjectOutput();
            var valueSize = (int)serializeValueSize;

            // Start with a full buffer plus two pages to overflow
            int numRec = ObjectsPerPage * (store.hlogBase.BufferSize + 2);
            int lastKey = 0;
            for (; lastKey < numRec; lastKey++)
            {
                var key = new TestObjectKey { key = lastKey };
                var value = new TestLargeObjectValue(valueSize);
                new Span<byte>(value.value).Fill((byte)key.key);
                _ = bContext.Upsert(key, ref input, value, ref output);
            }

            // Test that the expected number of pages were flushed and that we get the right results back. We've flushed in a multiple of full pages,
            // so ReadOnlyAddress will be aligned to page size. We have 0.1 mutable fraction, so we should have closed 2 pages and have 3 mutable,
            // leaving 27 immutable, totaling FUA 29 * PageSize. (That's why we verified 32 above.. easier to verify numbers here).
            // Note: This test does NOT use SizeTracker; therefore it does not evict except when page count is exceeded.
            Assert.That(IsAligned(store.hlogBase.ReadOnlyAddress, store.hlogBase.PageSize), $"ReadOnlyAddress ({store.hlogBase.ReadOnlyAddress}) should be page-aligned");
            Assert.That(store.hlogBase.FlushedUntilAddress, Is.EqualTo(store.hlogBase.PageSize * 29), $"FUA ({store.hlogBase.FlushedUntilAddress}) != PageSize * 29");  // <============== Fix this AF
            Assert.That(store.hlogBase.FlushedUntilAddress, Is.EqualTo(store.hlogBase.ReadOnlyAddress), $"FUA ({store.hlogBase.FlushedUntilAddress}) == ROA");
            DoRead(0, 2 * ObjectsPerPage - 1, onDisk: true);
            DoRead(2 * ObjectsPerPage, lastKey, onDisk: false);

            // Now make ReadOnlyAddress be no longer aligned to page: add half a page, the SetReadOnlyToTail. 
            const int ObjectsPerHalfPage = ObjectsPerPage / 2;
            for (var ii = 0; ii < ObjectsPerHalfPage; ii++)
            {
                var key = new TestObjectKey { key = lastKey + ii };
                var value = new TestLargeObjectValue(valueSize);
                new Span<byte>(value.value).Fill((byte)key.key);
                _ = bContext.Upsert(key, ref input, value, ref output);
            }

            store.epoch.Resume();
            try
            {
                Assert.That(store.hlogBase.ShiftReadOnlyToTail(out _, out var sroTask), Is.True);
                sroTask.Wait();
            }
            finally
            {
                store.epoch.Suspend();
            }

            // This should have flushed 2.5 more pages and Closed one more.
            Assert.That(!IsAligned(store.hlogBase.ReadOnlyAddress, store.hlogBase.PageSize), $"ReadOnlyAddress ({store.hlogBase.ReadOnlyAddress}) should not be page-aligned");
            Assert.That(store.hlogBase.FlushedUntilAddress, Is.EqualTo(store.hlogBase.PageSize * 34 + PageHeader.Size + ObjectsPerHalfPage * RecordLength), $"FUA ({store.hlogBase.FlushedUntilAddress}) != PageSize * 34 + ObjectsPerHalfPage");
            Assert.That(store.hlogBase.FlushedUntilAddress, Is.EqualTo(store.hlogBase.ReadOnlyAddress), $"FUA ({store.hlogBase.FlushedUntilAddress}) == ROA");
            Assert.That(store.hlogBase.FlushedUntilAddress, Is.EqualTo(store.hlogBase.GetTailAddress()), $"FUA ({store.hlogBase.FlushedUntilAddress}) == TA");
            DoRead(0, ObjectsPerPage * 3 - 1, onDisk: true);
            DoRead(ObjectsPerPage * 3, lastKey + ObjectsPerHalfPage, onDisk: false);

            void DoRead(int firstKey, int lastKey, bool onDisk)
            {
                TestLargeObjectInput input = new() { wantValueStyle = TestValueStyle.Object };
                for (int ii = firstKey; ii < lastKey; ii++)
                {
                    var output = new TestLargeObjectOutput();
                    var key = new TestObjectKey { key = ii };

                    var status = bContext.Read(key, ref input, ref output, Empty.Default);
                    Assert.That(status.IsPending, Is.EqualTo(onDisk), $"status.IsPending ({status}) != onDisk for key {ii}");
                    if (status.IsPending)
                        (status, output) = bContext.GetSinglePendingResult();

                    Assert.That(output.valueObject.value.Length, Is.EqualTo(valueSize));
                    var numLongs = output.valueObject.value.Length % 8;
                    var badIndex = new ReadOnlySpan<byte>(output.valueObject.value).IndexOfAnyExcept((byte)ii);
                    if (badIndex != -1)
                        Assert.Fail($"Unexpected byte value at index {badIndex}, onDisk {onDisk}, record# {ii}: {output.valueObject.value[badIndex]}");
                }
            }
        }

        [Test, Category(TsavoriteKVTestCategory), Category(LogRecordCategory), Category(SmokeTestCategory), Category(ObjectIdMapCategory)]
        //[Repeat(300)]
        // Note: This test name keys the mutableFraction
        public async Task LargeObjectLinearizeFlushedPages([Values(SerializeKeyValueSize.Thirty, SerializeKeyValueSize.OneK)] SerializeKeyValueSize serializeValueSize)
        {
            // Ensure our size calculations are correct by validating the test parameters are what we expect
            Assert.That(LogMemorySize, Is.EqualTo(1L << 15));
            Assert.That(PageSize, Is.EqualTo(1L << 10));
            Assert.That(store.hlogBase.BufferSize, Is.EqualTo(32));     // LogMemorySize is PageSize << 5
            Assert.That(store.hlogBase.MaxAllocatedPageCount, Is.EqualTo(32));
            const int RecordLength = 32;                                // LogRecord allocated size
            const int ObjectsPerPage = (int)((PageSize - PageHeader.Size) / RecordLength);
            Assert.That(ObjectsPerPage, Is.EqualTo(30));                // Make debugging easier by verifying the length we'll see in the IDE

            var functions = new TestLargeObjectFunctions { expectedRecordLength = RecordLength }; // ExpectedRecordLength controls how many objects per page
            using var session = store.NewSession<TestObjectKey, TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions>(functions);
            var bContext = session.BasicContext;

            var input = new TestLargeObjectInput();
            var output = new TestLargeObjectOutput();
            var valueSize = (int)serializeValueSize;

            // Verify initial conditions
            Assert.That(store.hlogBase.GetTailAddress(), Is.EqualTo(PageHeader.Size));
            Assert.That(store.hlogBase.HeadAddress, Is.EqualTo(PageHeader.Size));
            Assert.That(store.hlogBase.ReadOnlyAddress, Is.EqualTo(PageHeader.Size));
            Assert.That(store.hlogBase.FlushedUntilAddress, Is.EqualTo(PageHeader.Size));

            int numRec = ObjectsPerPage * store.hlogBase.BufferSize;
            int lastKey = 0;
            for (; lastKey < numRec; lastKey++)
            {
                var key = new TestObjectKey { key = lastKey };
                var value = new TestLargeObjectValue(valueSize);
                new Span<byte>(value.value).Fill((byte)key.key);
                _ = bContext.Upsert(key, ref input, value, ref output);
            }

            // Verify post-insert conditions.
            Assert.That(store.hlogBase.GetTailAddress(), Is.EqualTo(store.hlogBase.PageSize * store.hlogBase.BufferSize));
            Assert.That(store.hlogBase.HeadAddress, Is.EqualTo(PageHeader.Size));
            Assert.That(store.hlogBase.ReadOnlyAddress, Is.EqualTo(PageHeader.Size));
            Assert.That(store.hlogBase.FlushedUntilAddress, Is.EqualTo(PageHeader.Size));

            // Now test flushing in sequence so the second call has to wait to linearize. Use a semaphore to make sure we've
            // launched the first one before calling the second, else the second call may be run first.
            ManualResetEventSlim gate = new(false);
            var task = Task.Run(() => DoFlush((store.hlogBase.BufferSize - 20) * store.hlogBase.PageSize, gate));

            // We have to wait for this outside the epoch to avoid deadlock.
            gate.Wait();

            Task sroTask;
            store.epoch.Resume();
            try
            {
                Assert.That(store.hlogBase.ShiftReadOnlyToTail(out _, out sroTask), Is.True);
            }
            finally
            {
                store.epoch.Suspend();
            }
            gate.Dispose();

            await Task.WhenAll(task, sroTask);

            // Test that the FlushedUntilAddress is correct and that we get the right results back; nothing has been evicted yet, so all records are in memory.
            Assert.That(store.hlogBase.FlushedUntilAddress, Is.EqualTo(store.hlogBase.GetTailAddress()));
            Assert.That(store.hlogBase.ReadOnlyAddress, Is.EqualTo(store.hlogBase.FlushedUntilAddress), $"FUA ({store.hlogBase.FlushedUntilAddress}) == ROA");
            DoRead(0, ObjectsPerPage * store.hlogBase.BufferSize, onDisk: false);

            void DoRead(int firstKey, int lastKey, bool onDisk)
            {
                TestLargeObjectInput input = new() { wantValueStyle = TestValueStyle.Object };
                for (int ii = firstKey; ii < lastKey; ii++)
                {
                    var output = new TestLargeObjectOutput();
                    var key = new TestObjectKey { key = ii };

                    var status = bContext.Read(key, ref input, ref output, Empty.Default);
                    Assert.That(status.IsPending, Is.EqualTo(onDisk), $"status.IsPending ({status}) != onDisk for key {ii}");
                    if (status.IsPending)
                        (status, output) = bContext.GetSinglePendingResult();

                    Assert.That(output.valueObject.value.Length, Is.EqualTo(valueSize));
                    var numLongs = output.valueObject.value.Length % 8;
                    var badIndex = new ReadOnlySpan<byte>(output.valueObject.value).IndexOfAnyExcept((byte)ii);
                    if (badIndex != -1)
                        Assert.Fail($"Unexpected byte value at index {badIndex}, onDisk {onDisk}, record# {ii}: {output.valueObject.value[badIndex]}");
                }
            }

            void DoFlush(long newReadOnlyAddress, ManualResetEventSlim gate)
            {
                store.epoch.Resume();
                try
                {
                    // Do this in two pieces so we signal the gate in between to give the second call above a chance to launch.
                    Assert.That(store.hlogBase.ShiftReadOnlyAddress(newReadOnlyAddress - store.hlogBase.PageSize * 10), Is.True);
                    gate?.Set();
                    Assert.That(store.hlogBase.ShiftReadOnlyAddress(newReadOnlyAddress), Is.True);
                }
                finally
                {
                    store.epoch.Suspend();
                }
            }
        }

        [Test, Category(TsavoriteKVTestCategory), Category(LogRecordCategory), Category(SmokeTestCategory), Category(ObjectIdMapCategory)]
        public void ObjectDiskWriteReadOverflowValue()
        {
            using var session = store.NewSession<TestObjectKey, TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions>(new TestLargeObjectFunctions());
            var bContext = session.BasicContext;

            var valueSize = IStreamBuffer.BufferSize / 2;
            const int numRec = 5;
            var valueBuffer = new byte[valueSize * numRec];
            new Span<byte>(valueBuffer).Fill(0x42);

            for (int ii = 0; ii < numRec; ii++)
            {
                var key = new TestObjectKey { key = ii };
                var value = new ReadOnlySpan<byte>(valueBuffer).Slice(0, valueSize * (ii + 1));
                _ = bContext.Upsert(key, value, Empty.Default);
            }

            store.Log.FlushAndEvict(wait: true);

            TestLargeObjectInput input = new() { wantValueStyle = TestValueStyle.Overflow };

            for (int ii = 0; ii < numRec; ii++)
            {
                var output = new TestLargeObjectOutput();
                var key = new TestObjectKey { key = ii };

                input.expectedSpanLength = valueSize * (ii + 1);
                var status = bContext.Read(key, ref input, ref output, Empty.Default);
                if (status.IsPending)
                    (status, output) = bContext.GetSinglePendingResult();

                Assert.That(output.valueArray.Length, Is.EqualTo(valueSize * (ii + 1)));
                Assert.That(new ReadOnlySpan<byte>(output.valueArray).SequenceEqual(new ReadOnlySpan<byte>(valueBuffer).Slice(0, output.valueArray.Length)));
            }
        }

        [Test, Category(TsavoriteKVTestCategory), Category(LogRecordCategory), Category(SmokeTestCategory), Category(ObjectIdMapCategory)]
        //[Repeat(300)]
        public void LargeObjectDiskWriteReadBigKeyAndValue([Values] SerializeKeyValueSize serializeKeySize, [Values] SerializeKeyValueSize serializeValueSize)
        {
            using var session = store.NewSession<TestSpanByteKey, TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions>(new TestLargeObjectFunctions());
            var bContext = session.BasicContext;

            var input = new TestLargeObjectInput();
            var output = new TestLargeObjectOutput();
            var keySize = (int)serializeKeySize;
            var keyBuf = new byte[keySize];
            var valueSize = (int)serializeValueSize;
            const int numRec = 3;
            for (int ii = 0; ii < numRec; ii++)
            {
                var value = new TestLargeObjectValue(valueSize + (ii * 4096));
                var key = new Span<byte>(keyBuf);
                key.Fill((byte)(ii + 100));
                new Span<byte>(value.value).Fill(0x42);
                _ = bContext.Upsert(TestSpanByteKey.FromPinnedSpan(key), ref input, value, ref output);
            }

            // Test before and after the flush
            DoRead(onDisk: false);
            store.Log.FlushAndEvict(wait: true);
            DoRead(onDisk: true);

            void DoRead(bool onDisk)
            {
                TestLargeObjectInput input = new() { wantValueStyle = TestValueStyle.Object };
                for (int ii = 0; ii < numRec; ii++)
                {
                    var output = new TestLargeObjectOutput();
                    var key = new Span<byte>(keyBuf);
                    key.Fill((byte)(ii + 100));

                    var status = bContext.Read(TestSpanByteKey.FromPinnedSpan(key), ref input, ref output, Empty.Default);
                    Assert.That(status.IsPending, Is.EqualTo(onDisk));
                    if (status.IsPending)
                        (status, output) = bContext.GetSinglePendingResult();
                    Assert.That(status.Found, Is.True);

                    Assert.That(output.valueObject.value.Length, Is.EqualTo(valueSize + (ii * 4096)), $"record# ii {ii}");
                    var badIndex = new ReadOnlySpan<byte>(output.valueObject.value).IndexOfAnyExcept((byte)0x42);
                    if (badIndex != -1)
                        Assert.Fail($"Unexpected byte value at index {badIndex}, onDisk {onDisk}, record# {ii}: {output.valueObject.value[badIndex]}");
                }
            }
        }
    }
}