// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using NUnit.Framework;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test
{
    using TestAllocator = ObjectAllocator<StoreFunctions<SpanByteComparer, DefaultRecordTriggers>>;
    using TestStoreFunctions = StoreFunctions<SpanByteComparer, DefaultRecordTriggers>;

    [TestFixture]
    internal unsafe class PostUpsertRecordTests : TestBase
    {
        class LegacyFunctions : SessionFunctionsBase<int, int, Empty>
        {
            internal int spanCalls;
            internal int objectCalls;

            public override void PostUpsertOperation<TKey, TEpochAccessor>(TKey key, ref int input, ReadOnlySpan<byte> valueSpan,
                ref UpsertInfo upsertInfo, TEpochAccessor epochAccessor) => spanCalls++;

            public override void PostUpsertOperation<TKey, TEpochAccessor>(TKey key, ref int input, IHeapObject valueObject,
                ref UpsertInfo upsertInfo, TEpochAccessor epochAccessor) => objectCalls++;
        }

        class RecordFunctions : LegacyFunctions
        {
            internal int recordCalls;
            internal int successfulCalls;
            internal int initialCalls;
            internal int inPlaceCalls;
            internal bool cancel;
            internal bool forceFallback;

            public override bool InitialWriter<TSourceLogRecord>(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref int input,
                in TSourceLogRecord inputLogRecord, ref int output, ref UpsertInfo upsertInfo)
            {
                initialCalls++;
                if (cancel)
                {
                    upsertInfo.Action = UpsertAction.CancelOperation;
                    return false;
                }
                return base.InitialWriter(ref logRecord, in sizeInfo, ref input, in inputLogRecord, ref output, ref upsertInfo);
            }

            public override void PostInitialWriter<TSourceLogRecord>(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref int input,
                in TSourceLogRecord inputLogRecord, ref int output, ref UpsertInfo upsertInfo) => upsertInfo.UserData = 1;

            public override bool InPlaceWriter<TSourceLogRecord>(ref LogRecord logRecord, ref int input, in TSourceLogRecord inputLogRecord,
                ref int output, ref UpsertInfo upsertInfo)
            {
                inPlaceCalls++;
                if (cancel)
                {
                    upsertInfo.Action = UpsertAction.CancelOperation;
                    return false;
                }
                if (forceFallback)
                    return false;
                if (!base.InPlaceWriter(ref logRecord, ref input, in inputLogRecord, ref output, ref upsertInfo))
                    return false;
                upsertInfo.UserData = 1;
                return true;
            }

            public override void PostUpsertOperation<TKey, TSourceLogRecord, TEpochAccessor>(TKey key, ref int input,
                in TSourceLogRecord inputLogRecord, ref UpsertInfo upsertInfo, TEpochAccessor epochAccessor)
            {
                recordCalls++;
                Assert.That(key.KeyBytes.ToArray(), Is.EqualTo("destination"u8.ToArray()));
                Assert.That(inputLogRecord.Key.ToArray(), Is.EqualTo("source"u8.ToArray()));
                Assert.That(inputLogRecord.ETag, Is.EqualTo(37));
                Assert.That(inputLogRecord.Expiration, Is.EqualTo(123456789));
                if (upsertInfo.UserData == 1)
                    successfulCalls++;
            }
        }

        TsavoriteKV<TestStoreFunctions, TestAllocator> store;
        IDevice device;
        ObjectIdMap sourceObjects;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            device = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "callbacks.log"), deleteOnClose: true);
            store = new(new KVSettings
            {
                IndexSize = 1L << 16,
                LogDevice = device,
                PageSize = MinKvLogPageSize,
                LogMemorySize = 1L << 18,
                MaxInlineValueSize = 1024,
                RevivificationSettings = RevivificationSettings.InChainOnly
            }, StoreFunctions.Create(new SpanByteComparer(), new DefaultRecordTriggers()),
                (settings, functions) => new(settings, functions));
            sourceObjects = new();
        }

        [TearDown]
        public void TearDown()
        {
            store?.Dispose();
            sourceObjects?.Clear();
            device?.Dispose();
            OnTearDown();
        }

        LogRecord CreateSource(byte* pointer, int length, bool objectValue = false)
        {
            RecordSizeInfo sizeInfo = new()
            {
                FieldInfo = new()
                {
                    KeySize = "source"u8.Length,
                    ValueSize = objectValue ? ObjectIdMap.ObjectIdSize : length,
                    ValueIsObject = objectValue,
                    HasETag = true,
                    HasExpiration = true
                }
            };
            store.Log.PopulateRecordSizeInfo(ref sizeInfo);
            LogRecord record = new(pointer, sourceObjects) { InfoRef = RecordInfo.InitialValid };
            record.InitializeRecord(TestSpanByteKey.FromPinnedSpan("source"u8), in sizeInfo, sourceObjects);
            if (objectValue)
                Assert.That(record.TrySetValueObjectAndPrepareOptionals(new TestObjectValue { value = 42 }, in sizeInfo), Is.True);
            else
                Assert.That(record.TrySetValueSpanAndPrepareOptionals(new byte[length], in sizeInfo), Is.True);
            Assert.That(record.TrySetETag(37), Is.True);
            Assert.That(record.TrySetExpiration(123456789), Is.True);
            return record;
        }

        [TestCase("initial")]
        [TestCase("in-place")]
        [TestCase("growth")]
        [TestCase("read-only")]
        [TestCase("revivification")]
        [TestCase("cancel-initial")]
        [TestCase("cancel-in-place")]
        public void RecordCallbackReceivesSourceAtFinalExecutionPoint(string route)
        {
            RecordFunctions functions = new();
            using ClientSession<TestSpanByteKey, int, int, Empty, RecordFunctions, TestStoreFunctions, TestAllocator> session =
                store.NewSession<TestSpanByteKey, int, int, Empty, RecordFunctions>(functions);
            BasicContext<TestSpanByteKey, int, int, Empty, RecordFunctions, TestStoreFunctions, TestAllocator> context = session.BasicContext;
            TestSpanByteKey key = TestSpanByteKey.FromPinnedSpan("destination"u8);
            if (route is not ("initial" or "cancel-initial"))
                context.Upsert(key, new byte[route == "revivification" ? 128 : 32]);
            if (route == "read-only")
                store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
            if (route == "revivification")
                context.Delete(key);
            functions.cancel = route.StartsWith("cancel", StringComparison.Ordinal);
            functions.forceFallback = route == "growth";
            byte* buffer = stackalloc byte[2048];
            LogRecord source = CreateSource(buffer, route == "growth" ? 512 : 32);
            int input = 0;
            Status status = context.Upsert(key, ref input, in source);
            Assert.That(status.IsCanceled, Is.EqualTo(functions.cancel));
            Assert.That(functions.recordCalls, Is.EqualTo(1));
            Assert.That(functions.successfulCalls, Is.EqualTo(functions.cancel ? 0 : 1));
            Assert.That(functions.spanCalls, Is.EqualTo(route is "initial" or "cancel-initial" ? 0 : 1));
            if (route is "in-place" or "cancel-in-place")
                Assert.That(functions.inPlaceCalls, Is.EqualTo(1));
            if (route == "growth")
            {
                Assert.That(functions.inPlaceCalls, Is.EqualTo(1));
                Assert.That(functions.initialCalls, Is.EqualTo(1));
            }
            if (route == "revivification")
            {
                Assert.That(status.Record.InPlaceUpdated, Is.True);
                Assert.That(functions.initialCalls, Is.EqualTo(1));
            }
        }

        [Test]
        public void DefaultRecordForwardingPreservesLegacyValueOverrides([Values] bool objectValue)
        {
            LegacyFunctions functions = new();
            using ClientSession<TestSpanByteKey, int, int, Empty, LegacyFunctions, TestStoreFunctions, TestAllocator> session =
                store.NewSession<TestSpanByteKey, int, int, Empty, LegacyFunctions>(functions);
            BasicContext<TestSpanByteKey, int, int, Empty, LegacyFunctions, TestStoreFunctions, TestAllocator> context = session.BasicContext;
            byte* buffer = stackalloc byte[2048];
            LogRecord source = CreateSource(buffer, 32, objectValue);
            int input = 0;
            context.Upsert(TestSpanByteKey.FromPinnedSpan("record"u8), ref input, in source);
            Assert.That(functions.spanCalls, Is.EqualTo(objectValue ? 0 : 1));
            Assert.That(functions.objectCalls, Is.EqualTo(objectValue ? 1 : 0));
            if (objectValue)
                context.Upsert(TestSpanByteKey.FromPinnedSpan("ordinary"u8), new TestObjectValue { value = 43 });
            else
                context.Upsert(TestSpanByteKey.FromPinnedSpan("ordinary"u8), new byte[32]);
            Assert.That(functions.spanCalls, Is.EqualTo(objectValue ? 0 : 2));
            Assert.That(functions.objectCalls, Is.EqualTo(objectValue ? 2 : 0));
        }
    }
}