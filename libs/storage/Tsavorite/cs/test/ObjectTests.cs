// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.IO;
using Allure.NUnit;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;

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

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "ObjectTests.log"), deleteOnClose: true);
            objlog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "ObjectTests.obj.log"), deleteOnClose: true);
            var storeFunctions = TestContext.CurrentContext.Test.MethodName.StartsWith("ObjectDiskWriteReadLarge")
                ? StoreFunctions.Create(new TestObjectKey.Comparer(), () => new TestLargeObjectValue.Serializer(), DefaultRecordDisposer.Instance)
                : StoreFunctions.Create(new TestObjectKey.Comparer(), () => new TestObjectValue.Serializer(), DefaultRecordDisposer.Instance);

            store = new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                ObjectLogDevice = objlog,
                MutableFraction = 0.1,
                LogMemorySize = 1L << 15,
                PageSize = 1L << 10
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
            DeleteDirectory(MethodTestDir);
        }

        [Test, Category(TsavoriteKVTestCategory), Category(SmokeTestCategory), Category(ObjectIdMapCategory)]
        public void ObjectInMemWriteReadUpsert()
        {
            using var session = store.NewSession<TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions());
            var bContext = session.BasicContext;

            TestObjectKey keyStruct = new() { key = 9999999 };
            var key = SpanByte.FromPinnedVariable(ref keyStruct);
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
            using var session = store.NewSession<TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions());
            var bContext = session.BasicContext;

            TestObjectKey key1Struct = new() { key = 8999998 };
            var key1 = SpanByte.FromPinnedVariable(ref key1Struct);
            TestObjectInput input1 = new() { value = 23 };
            TestObjectOutput output = new();

            _ = bContext.RMW(key1, ref input1, Empty.Default);

            TestObjectKey key2Struct = new() { key = 8999999 };
            var key2 = SpanByte.FromPinnedVariable(ref key2Struct);
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
            using var session = store.NewSession<TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions());
            var bContext = session.BasicContext;
            const int keyInt = 42;

            var keyStruct = new TestObjectKey { key = keyInt };
            var key = SpanByte.FromPinnedVariable(ref keyStruct);
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
            using var session = store.NewSession<TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions());
            var bContext = session.BasicContext;

            for (int i = 0; i < 2000; i++)
            {
                var key1Struct = new TestObjectKey { key = i };
                var key = SpanByte.FromPinnedVariable(ref key1Struct);
                var value = new TestObjectValue { value = i };
                if (i == 120)
                    i += 0;
                _ = bContext.Upsert(key, value, Empty.Default);
            }

            TestObjectKey key2Struct = new() { key = 23 };
            var key2 = SpanByte.FromPinnedVariable(ref key2Struct);
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

            key2Struct.key = 99999;
            status = bContext.Read(key2, ref input, ref g1, Empty.Default);

            if (status.IsPending)
                (status, _) = bContext.GetSinglePendingResult();
            ClassicAssert.IsFalse(status.Found);

            // Update last 100 using RMW in memory
            for (int i = 1900; i < 2000; i++)
            {
                var keyStruct = new TestObjectKey { key = i };
                var key = SpanByte.FromPinnedVariable(ref keyStruct);
                input = new TestObjectInput { value = 1 };
                status = bContext.RMW(key, ref input, Empty.Default);
                ClassicAssert.IsFalse(status.IsPending, "Expected RMW to complete in-memory");
            }

            // Update first 100 using RMW from storage
            var numPendingUpdates = 0;
            for (int i = 0; i < 100; i++)
            {
                var keyStruct = new TestObjectKey { key = i };
                var key = SpanByte.FromPinnedVariable(ref keyStruct);
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
                var keyStruct = new TestObjectKey { key = i };
                var key = SpanByte.FromPinnedVariable(ref keyStruct);
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
        public void ObjectDiskWriteReadLargeValueSmallKey([Values] SerializeKeyValueSize serializeValueSize)
        {
            if (TestContext.CurrentContext.CurrentRepeatCount > 0)
                Debug.WriteLine($"*** Current test iteration: {TestContext.CurrentContext.CurrentRepeatCount + 1} ***");

            using var session = store.NewSession<TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions>(new TestLargeObjectFunctions());
            var bContext = session.BasicContext;

            var input = new TestLargeObjectInput();
            var output = new TestLargeObjectOutput();
            var valueSize = (int)serializeValueSize;
            const int numRec = 3;
            for (int ii = 0; ii < numRec; ii++)
            {
                var key1Struct = new TestObjectKey { key = ii };
                var key = SpanByte.FromPinnedVariable(ref key1Struct);
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
                    var keyStruct = new TestObjectKey { key = ii };
                    var key = SpanByte.FromPinnedVariable(ref keyStruct);

                    var status = bContext.Read(key, ref input, ref output, Empty.Default);
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
        public void ObjectDiskWriteReadOverflowValue()
        {
            using var session = store.NewSession<TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions>(new TestLargeObjectFunctions());
            var bContext = session.BasicContext;

            var valueSize = IStreamBuffer.BufferSize / 2;
            const int numRec = 5;
            var valueBuffer = new byte[valueSize * numRec];
            new Span<byte>(valueBuffer).Fill(0x42);

            for (int ii = 0; ii < numRec; ii++)
            {
                var key1Struct = new TestObjectKey { key = ii };
                var key = SpanByte.FromPinnedVariable(ref key1Struct);
                var value = new ReadOnlySpan<byte>(valueBuffer).Slice(0, valueSize * (ii + 1));
                _ = bContext.Upsert(key, value, Empty.Default);
            }

            store.Log.FlushAndEvict(wait: true);

            TestLargeObjectInput input = new() { wantValueStyle = TestValueStyle.Overflow };

            for (int ii = 0; ii < numRec; ii++)
            {
                var output = new TestLargeObjectOutput();
                var keyStruct = new TestObjectKey { key = ii };
                var key = SpanByte.FromPinnedVariable(ref keyStruct);

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
        public void ObjectDiskWriteReadLargeKeyAndValue([Values] SerializeKeyValueSize serializeKeySize, [Values] SerializeKeyValueSize serializeValueSize)
        {
            if (TestContext.CurrentContext.CurrentRepeatCount > 0)
                Debug.WriteLine($"*** Current test iteration: {TestContext.CurrentContext.CurrentRepeatCount + 1} ***");

            using var session = store.NewSession<TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions>(new TestLargeObjectFunctions());
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
                    var key = new Span<byte>(keyBuf);
                    key.Fill((byte)(ii + 100));

                    var status = bContext.Read(key, ref input, ref output, Empty.Default);
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