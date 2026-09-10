// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using Garnet.test;
using NUnit.Framework;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.Objects
{
    using TraceAllocator = ObjectAllocator<StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>>;
    using TraceSpanByteKeyAllocator = ObjectAllocator<StoreFunctions<SpanByteComparer, DefaultRecordTriggers>>;
    using TraceSpanByteKeyStoreFunctions = StoreFunctions<SpanByteComparer, DefaultRecordTriggers>;
    using TraceStoreFunctions = StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>;

    /// <summary>
    /// Small, one-record tests intended for stepping through the ObjectAllocator ReadOnly flush path.
    /// </summary>
    [TestFixture]
    internal class ObjectReadOnlyFlushTraceTests : TestBase
    {
        const int InlineValueLimit = 16 * 1024;

        // The 511/512 pair confirms that inline data bypasses object-log framing at the same sizes that split its exact/headered encodings.
        static readonly int[] InlineValueSizes = [1, 511, 512, 4096, InlineValueLimit];

        // These cover headerless exact data, the first headered size, buffered/direct-write boundaries, and a multi-buffer-sized payload.
        static readonly int[] OverflowValueSizes = [1, 511, 512, 4096, 128 * 1024, (128 * 1024) + 1, (4 * 1024 * 1024) + 1];
        static readonly int[] OverflowKeySizes = [4, 511, 512, 4096, 128 * 1024, (128 * 1024) + 1, (4 * 1024 * 1024) + 1];

        // TestLargeObjectValue.Serializer prefixes the payload with a four-byte length, so payloads 507 and 508
        // produce serialized lengths 511 and 512 respectively.
        static readonly int[] ObjectValueSizes = [1, 507, 508, 64 * 1024, (4 * 1024 * 1024) + 1];

        [SetUp]
        public void Setup() => RecreateDirectory(MethodTestDir);

        [TearDown]
        public void TearDown() => OnTearDown();

        static TsavoriteKV<TraceStoreFunctions, TraceAllocator> CreateStore(IDevice log, IDevice objectLog, int maxInlineValueSize)
            => new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                ObjectLogDevice = objectLog,
                MutableFraction = 0.9,
                PageSize = 1L << 16,
                LogMemorySize = 1L << 20,
                SegmentSize = 1L << 20,
                ObjectLogSegmentSize = 1L << 30,
                MaxInlineValueSize = maxInlineValueSize,
            }, StoreFunctions.Create(new TestObjectKey.Comparer(), () => new TestLargeObjectValue.Serializer(), DefaultRecordTriggers.Instance),
               (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));

        static TsavoriteKV<TraceSpanByteKeyStoreFunctions, TraceSpanByteKeyAllocator> CreateOverflowKeyStore(IDevice log, IDevice objectLog)
            => new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                ObjectLogDevice = objectLog,
                MutableFraction = 0.9,
                PageSize = 1L << 16,
                LogMemorySize = 1L << 20,
                SegmentSize = 1L << 20,
                ObjectLogSegmentSize = 1L << 30,
                MaxInlineKeySize = 0,
                MaxInlineValueSize = InlineValueLimit,
            }, StoreFunctions.Create(new SpanByteComparer(), () => new TestLargeObjectValue.Serializer(), DefaultRecordTriggers.Instance),
               (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));

        static byte[] MakePayload(int size)
        {
            var payload = new byte[size];
            for (var index = 0; index < payload.Length; index++)
                payload[index] = (byte)(index * 31);
            return payload;
        }

        [Test]
        [Category(TsavoriteKVTestCategory), Category(ObjectIdMapCategory)]
        public void ReadOnlyFlushInlineValue([ValueSource(nameof(InlineValueSizes))] int valueSize)
        {
            using var log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "inline.log"), deleteOnClose: true);
            using var objectLog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "inline.obj.log"), deleteOnClose: true);
            using var store = CreateStore(log, objectLog, InlineValueLimit);
            using var session = store.NewSession<TestObjectKey, TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions>(new());
            var context = session.BasicContext;
            var key = new TestObjectKey { key = valueSize };
            var expected = MakePayload(valueSize);

            Assert.That(context.Upsert(key, expected.AsSpan(), Empty.Default).IsPending, Is.False);
            Verify(TestValueStyle.Inline);

            var flushUntilAddress = store.Log.TailAddress;
            store.Log.ShiftReadOnlyAddress(flushUntilAddress, wait: true);

            Assert.That(store.Log.FlushedUntilAddress, Is.GreaterThanOrEqualTo(flushUntilAddress));
            Verify(TestValueStyle.Inline);

            void Verify(TestValueStyle expectedStyle)
            {
                var input = new TestLargeObjectInput { wantValueStyle = expectedStyle, expectedSpanLength = valueSize };
                var output = new TestLargeObjectOutput();
                var status = context.Read(key, ref input, ref output, Empty.Default);
                Assert.That(status.Found, Is.True);
                Assert.That(status.IsPending, Is.False);
                Assert.That(output.valueArray, Is.EqualTo(expected));
            }
        }

        [Test]
        [Category(TsavoriteKVTestCategory), Category(ObjectIdMapCategory)]
        public void ReadOnlyFlushOverflowValue([ValueSource(nameof(OverflowValueSizes))] int valueSize)
        {
            using var log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "overflow.log"), deleteOnClose: true);
            using var objectLog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "overflow.obj.log"), deleteOnClose: true);
            using var store = CreateStore(log, objectLog, maxInlineValueSize: 0);
            using var session = store.NewSession<TestObjectKey, TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions>(new());
            var context = session.BasicContext;
            var key = new TestObjectKey { key = valueSize };
            var expected = MakePayload(valueSize);

            Assert.That(context.Upsert(key, expected.AsSpan(), Empty.Default).IsPending, Is.False);
            Verify();

            var flushUntilAddress = store.Log.TailAddress;
            store.Log.ShiftReadOnlyAddress(flushUntilAddress, wait: true);

            Assert.That(store.Log.FlushedUntilAddress, Is.GreaterThanOrEqualTo(flushUntilAddress));
            Verify();

            void Verify()
            {
                var input = new TestLargeObjectInput { wantValueStyle = TestValueStyle.Overflow, expectedSpanLength = valueSize };
                var output = new TestLargeObjectOutput();
                var status = context.Read(key, ref input, ref output, Empty.Default);
                Assert.That(status.Found, Is.True);
                Assert.That(status.IsPending, Is.False);
                Assert.That(output.valueArray, Is.EqualTo(expected));
            }
        }

        [Test]
        [Category(TsavoriteKVTestCategory), Category(ObjectIdMapCategory)]
        public void ReadOnlyFlushOverflowKey([ValueSource(nameof(OverflowKeySizes))] int keySize)
        {
            using var log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "overflow-key.log"), deleteOnClose: true);
            using var objectLog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "overflow-key.obj.log"), deleteOnClose: true);
            using var store = CreateOverflowKeyStore(log, objectLog);
            using var session = store.NewSession<TestSpanByteKey, TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions>(new());
            var context = session.BasicContext;
            var keyBytes = MakePayload(keySize);
            _ = BitConverter.TryWriteBytes(keyBytes, keySize);
            var expected = MakePayload(32);

            Assert.That(context.Upsert(TestSpanByteKey.FromArray(keyBytes), expected.AsSpan(), Empty.Default).IsPending, Is.False);
            Verify();

            var flushUntilAddress = store.Log.TailAddress;
            store.Log.ShiftReadOnlyAddress(flushUntilAddress, wait: true);

            Assert.That(store.Log.FlushedUntilAddress, Is.GreaterThanOrEqualTo(flushUntilAddress));
            Verify();

            void Verify()
            {
                var input = new TestLargeObjectInput { wantValueStyle = TestValueStyle.Inline, expectedSpanLength = expected.Length };
                var output = new TestLargeObjectOutput();
                var status = context.Read(TestSpanByteKey.FromArray(keyBytes), ref input, ref output, Empty.Default);
                Assert.That(status.Found, Is.True);
                Assert.That(status.IsPending, Is.False);
                Assert.That(output.valueArray, Is.EqualTo(expected));
            }
        }

        [Test]
        [Category(TsavoriteKVTestCategory), Category(ObjectIdMapCategory)]
        public void ReadOnlyFlushObjectValue([ValueSource(nameof(ObjectValueSizes))] int valueSize)
        {
            using var log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "object.log"), deleteOnClose: true);
            using var objectLog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "object.obj.log"), deleteOnClose: true);
            using var store = CreateStore(log, objectLog, InlineValueLimit);
            using var session = store.NewSession<TestObjectKey, TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions>(new());
            var context = session.BasicContext;
            var key = new TestObjectKey { key = valueSize };
            var expected = MakePayload(valueSize);
            var value = new TestLargeObjectValue { value = expected };
            var input = new TestLargeObjectInput();
            var output = new TestLargeObjectOutput();

            Assert.That(context.Upsert(key, ref input, value, ref output).IsPending, Is.False);
            Verify();

            var flushUntilAddress = store.Log.TailAddress;
            store.Log.ShiftReadOnlyAddress(flushUntilAddress, wait: true);

            Assert.That(store.Log.FlushedUntilAddress, Is.GreaterThanOrEqualTo(flushUntilAddress));
            Verify();

            void Verify()
            {
                var readInput = new TestLargeObjectInput { wantValueStyle = TestValueStyle.Object };
                var readOutput = new TestLargeObjectOutput();
                var status = context.Read(key, ref readInput, ref readOutput, Empty.Default);
                Assert.That(status.Found, Is.True);
                Assert.That(status.IsPending, Is.False);
                Assert.That(readOutput.valueObject.value, Is.EqualTo(expected));
            }
        }

        /// <summary>
        /// Disposing a record frees its <see cref="ObjectIdMap"/> slot and then stores <see cref="ObjectIdMap.InvalidObjectId"/> into
        /// the record's field. Because the no-copy object-log flush resolves those slots from the LIVE page, it can observe either
        /// state while a concurrent Upsert/RMW elides the record; it must therefore be able to detect the loss rather than fault the
        /// flush thread, which is exactly what the unchecked <see cref="ObjectIdMap.GetOverflowByteArray"/> accessor does.
        /// </summary>
        [Test]
        [Category(TsavoriteKVTestCategory), Category(ObjectIdMapCategory)]
        public void TryGetOverflowByteArrayToleratesConcurrentSlotRelease()
        {
            var objectIdMap = new ObjectIdMap();
            var objectId = objectIdMap.AllocateAndSet(OverflowByteArray.AllocateData(64));

            Assert.That(objectIdMap.TryGetOverflowByteArray(objectId, out var captured), Is.True);
            Assert.That(captured.IsEmpty, Is.False);
            Assert.That(captured.Length, Is.EqualTo(64));

            // The capturing flusher's copy stays usable after the slot is released; a later lookup of the same slot does not.
            objectIdMap.Free(objectId);
            Assert.That(captured.Length, Is.EqualTo(64), "a captured overflow must survive release of its slot");
            Assert.That(objectIdMap.TryGetOverflowByteArray(objectId, out var afterFree), Is.False);
            Assert.That(afterFree.IsEmpty, Is.True);

            // The record's field is stamped with InvalidObjectId after the slot is freed, which is out of range for the backing array.
            Assert.That(objectIdMap.TryGetOverflowByteArray(ObjectIdMap.InvalidObjectId, out var afterClear), Is.False);
            Assert.That(afterClear.IsEmpty, Is.True);
            Assert.That(() => objectIdMap.GetOverflowByteArray(ObjectIdMap.InvalidObjectId).Length, Throws.Exception,
                "the unchecked accessor faults on a cleared field, which is why the flush capture must use the Try form");
        }
    }
}