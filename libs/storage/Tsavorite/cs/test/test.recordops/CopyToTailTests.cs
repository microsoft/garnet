// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.IO;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.CopyToTail
{
    using ClassAllocator = ObjectAllocator<StoreFunctions<SpanByteComparer, DefaultRecordTriggers>>;
    using ClassStoreFunctions = StoreFunctions<SpanByteComparer, DefaultRecordTriggers>;

    /// <summary>
    /// Exercises copying records read from disk to the main-log tail (<see cref="ReadCopyTo.MainLog"/>) and to the
    /// read cache (<see cref="ReadCopyTo.ReadCache"/>) across the key/value shapes the unified store supports:
    /// inline and overflow keys combined with inline, overflow, and object values.
    ///
    /// All shapes live in a single <see cref="ObjectAllocator{TStoreFunctions}"/> store (the only allocator that
    /// holds inline, overflow, AND object values) keyed by <see cref="SpanByteComparer"/>; the inline/overflow
    /// value cases use a <see cref="SpanByteFunctions{TContext}"/> session, the object value cases a
    /// <see cref="TestObjectFunctions"/> session.
    ///
    /// Each case verifies that the on-disk record is read pending, copied to the requested destination (the
    /// status reports <see cref="RecordStatus.Copied"/> / <see cref="RecordStatus.CopiedToReadCache"/> and the
    /// corresponding tail advances), the copied value is correct, and a subsequent read is served from memory.
    /// 
    /// TODO: Add extended namespaces to verify they copy as well.
    /// </summary>
    [TestFixture]
    class CopyToTailTests : TestBase
    {
        const int NumRecords = 200;
        const int ValueMult = 1_000_000;

        // Small inline limits so moderate spans overflow.
        const int MaxInlineKeyBytes = 16;
        const int MaxInlineValueBytes = 32;
        const int TestRecordType = 42;

        private TsavoriteKV<ClassStoreFunctions, ClassAllocator> store;
        private IDevice log, objlog;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            // The store is created per-test via CreateStore() so the read cache is enabled only when needed.
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

        private void CreateStore(ReadCopyTo readCopyTo)
        {
            log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "test.log"), deleteOnClose: true);
            objlog = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "test.obj.log"), deleteOnClose: true);

            var settings = new KVSettings
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                ObjectLogDevice = objlog,
                MutableFraction = 0.1,
                LogMemorySize = 1L << 20,
                PageSize = MinKvLogPageSize,
                MaxInlineKeySize = MaxInlineKeyBytes,
                MaxInlineValueSize = MaxInlineValueBytes,
            };

            // ReadCopyTo.ReadCache requires the read cache to be configured on the store ctor.
            if (readCopyTo == ReadCopyTo.ReadCache)
            {
                settings.ReadCacheEnabled = true;
                settings.ReadCacheMemorySize = 1L << 20;
                settings.ReadCachePageSize = MinKvLogPageSize;
            }

            store = new(settings,
                StoreFunctions.Create(new SpanByteComparer(), () => new TestObjectValue.Serializer(), DefaultRecordTriggers.Instance),
                (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));
        }

        // First 4 bytes carry the (unique) key int; the remainder stays zero so regeneration is deterministic.
        private static void MakeKey(Span<byte> dst, int key)
        {
            dst.Clear();
            _ = BitConverter.TryWriteBytes(dst, key);
        }

        private static void MakeValue(Span<byte> dst, int key)
        {
            dst.Clear();
            _ = BitConverter.TryWriteBytes(dst, key + ValueMult);
        }

        /// <summary>Inline/overflow (SpanByte) value flavor, parameterized by key/value span lengths.</summary>
        private void RunSpanByteTest(ReadCopyTo readCopyTo, int keyLen, int valueLen)
        {
            CreateStore(readCopyTo);
            using var session = store.NewSession<TestSpanByteKey, PinnedSpanByte, SpanByteAndMemory, Empty, CTTSpanByteFunctions>(new CTTSpanByteFunctions(TestRecordType));
            var bContext = session.BasicContext;

            Span<byte> keyBuf = stackalloc byte[keyLen];
            Span<byte> valueBuf = stackalloc byte[valueLen];

            for (var k = 0; k < NumRecords; k++)
            {
                MakeKey(keyBuf, k);
                MakeValue(valueBuf, k);
                ClassicAssert.IsFalse(bContext.Upsert(TestSpanByteKey.FromPinnedSpan(keyBuf), valueBuf).IsPending);
            }

            store.Log.FlushAndEvict(wait: true);

            var readKey = NumRecords / 2;
            MakeKey(keyBuf, readKey);
            MakeValue(valueBuf, readKey);   // valueBuf now holds the expected value bytes

            var tailBefore = store.Log.TailAddress;
            var rcTailBefore = readCopyTo == ReadCopyTo.ReadCache ? store.ReadCache.TailAddress : 0L;

            PinnedSpanByte input = default;
            SpanByteAndMemory output = default;
            ReadOptions readOptions = new() { CopyOptions = new(ReadCopyFrom.AllImmutable, readCopyTo) };

            // Record is on disk after FlushAndEvict, so the read goes pending; the copy happens on completion.
            var status = bContext.Read(TestSpanByteKey.FromPinnedSpan(keyBuf), ref input, ref output, ref readOptions, out _);
            ClassicAssert.IsTrue(status.IsPending, $"Expected pending read from disk; {status}");

            _ = bContext.CompletePendingWithOutputs(out var completedOutputs, wait: true);
            (status, output) = GetSinglePendingResult(completedOutputs);

            ClassicAssert.IsTrue(status.Found, $"Read not Found; {status}");
            ClassicAssert.IsNotNull(output.Memory, "Expected heap-copied output value");
            ClassicAssert.IsTrue(output.Span.SequenceEqual(valueBuf), "Copied value bytes mismatch");
            output.Memory?.Dispose();

            AssertCopiedToExpectedLocation(status, readCopyTo, tailBefore, rcTailBefore);

            // Second read with no copy options must now be served from memory (not pending) at the copy destination.
            output = default;
            ReadOptions noCopy = new() { CopyOptions = ReadCopyOptions.None };
            status = bContext.Read(TestSpanByteKey.FromPinnedSpan(keyBuf), ref input, ref output, ref noCopy, out _);
            ClassicAssert.IsFalse(status.IsPending, $"Second read should be served from memory after copy; {status}");
            ClassicAssert.IsTrue(status.Found, $"Second read not Found; {status}");
            ClassicAssert.IsTrue(output.Span.SequenceEqual(valueBuf), "In-memory value bytes mismatch on second read");
            output.Memory?.Dispose();
        }

        /// <summary>Object value flavor, parameterized by key span length (inline or overflow). The first 4 bytes
        /// always carry the key int, which <see cref="TestObjectFunctions"/> reads back as <c>TestObjectKey</c>.</summary>
        private void RunObjectTest(ReadCopyTo readCopyTo, int keyLen)
        {
            CreateStore(readCopyTo);
            using var session = store.NewSession<TestSpanByteKey, TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions());
            var bContext = session.BasicContext;

            Span<byte> keyBuf = stackalloc byte[keyLen];

            for (var k = 0; k < NumRecords; k++)
            {
                MakeKey(keyBuf, k);
                // value == key keeps TestObjectFunctions.ReadCompletionCallback's invariant valid.
                var valueObj = new TestObjectValue { value = k };
                ClassicAssert.IsFalse(bContext.Upsert(TestSpanByteKey.FromPinnedSpan(keyBuf), valueObj).IsPending);
            }

            store.Log.FlushAndEvict(wait: true);

            var readKey = NumRecords / 2;
            MakeKey(keyBuf, readKey);

            var tailBefore = store.Log.TailAddress;
            var rcTailBefore = readCopyTo == ReadCopyTo.ReadCache ? store.ReadCache.TailAddress : 0L;

            TestObjectInput input = default;
            TestObjectOutput output = default;
            ReadOptions readOptions = new() { CopyOptions = new(ReadCopyFrom.AllImmutable, readCopyTo) };

            var status = bContext.Read(TestSpanByteKey.FromPinnedSpan(keyBuf), ref input, ref output, ref readOptions, out _);
            ClassicAssert.IsTrue(status.IsPending, $"Expected pending read from disk; {status}");

            _ = bContext.CompletePendingWithOutputs(out var completedOutputs, wait: true);
            (status, output) = GetSinglePendingResult(completedOutputs);

            ClassicAssert.IsTrue(status.Found, $"Read not Found; {status}");
            ClassicAssert.IsNotNull(output.value, "Object value was not read");
            ClassicAssert.AreEqual(readKey, output.value.value, "Object value mismatch after copy");

            AssertCopiedToExpectedLocation(status, readCopyTo, tailBefore, rcTailBefore);

            // Second read with no copy options must now be served from memory (not pending) at the copy destination.
            output = default;
            ReadOptions noCopy = new() { CopyOptions = ReadCopyOptions.None };
            status = bContext.Read(TestSpanByteKey.FromPinnedSpan(keyBuf), ref input, ref output, ref noCopy, out _);
            ClassicAssert.IsFalse(status.IsPending, $"Second read should be served from memory after copy; {status}");
            ClassicAssert.IsTrue(status.Found, $"Second read not Found; {status}");
            ClassicAssert.AreEqual(readKey, output.value.value, "In-memory object value mismatch on second read");
        }

        private void AssertCopiedToExpectedLocation(Status status, ReadCopyTo readCopyTo, long tailBefore, long rcTailBefore)
        {
            if (readCopyTo == ReadCopyTo.MainLog)
            {
                ClassicAssert.IsTrue(status.Record.Copied, $"Expected CopiedRecord (to main log); {status}");
                ClassicAssert.Greater(store.Log.TailAddress, tailBefore, "Main-log tail did not grow after CopyToTail");
            }
            else
            {
                ClassicAssert.IsTrue(status.Record.CopiedToReadCache, $"Expected CopiedRecordToReadCache; {status}");
                ClassicAssert.Greater(store.ReadCache.TailAddress, rcTailBefore, "Read-cache tail did not grow after CopyToReadCache");
            }
        }

        [Test]
        [Category(ReadTestCategory), Category(ReadCacheTestCategory)]
        public void InlineKeyInlineValueCopyToTail([Values(ReadCopyTo.MainLog, ReadCopyTo.ReadCache)] ReadCopyTo readCopyTo)
            => RunSpanByteTest(readCopyTo, keyLen: sizeof(int), valueLen: MaxInlineValueBytes / 2);

        [Test]
        [Category(ReadTestCategory), Category(ReadCacheTestCategory)]
        public void InlineKeyOverflowValueCopyToTail([Values(ReadCopyTo.MainLog, ReadCopyTo.ReadCache)] ReadCopyTo readCopyTo)
            => RunSpanByteTest(readCopyTo, keyLen: sizeof(int), valueLen: MaxInlineValueBytes * 2);

        [Test]
        [Category(ReadTestCategory), Category(ReadCacheTestCategory)]
        public void OverflowKeyOverflowValueCopyToTail([Values(ReadCopyTo.MainLog, ReadCopyTo.ReadCache)] ReadCopyTo readCopyTo)
            => RunSpanByteTest(readCopyTo, keyLen: MaxInlineKeyBytes * 2, valueLen: MaxInlineValueBytes * 2);

        [Test]
        [Category(ReadTestCategory), Category(ReadCacheTestCategory)]
        public void InlineKeyObjectValueCopyToTail([Values(ReadCopyTo.MainLog, ReadCopyTo.ReadCache)] ReadCopyTo readCopyTo)
            => RunObjectTest(readCopyTo, keyLen: sizeof(int));

        [Test]
        [Category(ReadTestCategory), Category(ReadCacheTestCategory)]
        public void OverflowKeyObjectValueCopyToTail([Values(ReadCopyTo.MainLog, ReadCopyTo.ReadCache)] ReadCopyTo readCopyTo)
            => RunObjectTest(readCopyTo, keyLen: MaxInlineKeyBytes * 2);
    }

    public class CTTSpanByteFunctions : SessionFunctionsBase<PinnedSpanByte, SpanByteAndMemory, Empty>
    {
        protected readonly MemoryPool<byte> memoryPool;
        internal byte recordType = 0;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="recordType"></param>
        public CTTSpanByteFunctions(byte recordType)
        {
            this.recordType = recordType;
            this.memoryPool = MemoryPool<byte>.Shared;
        }

        /// <inheritdoc />
        public override bool Reader<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref PinnedSpanByte input, ref SpanByteAndMemory output, ref ReadInfo readInfo)
        {
            Assert.That(srcLogRecord.RecordType, Is.EqualTo(recordType));
            srcLogRecord.ValueSpan.CopyTo(ref output, memoryPool);
            return true;
        }

        /// <inheritdoc/>
        public override RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref PinnedSpanByte input)
            => new() { KeySize = srcLogRecord.Key.Length, ValueSize = input.Length };
        /// <inheritdoc/>
        public override RecordFieldInfo GetRMWInitialFieldInfo<TKey>(TKey key, ref PinnedSpanByte input)
            // TODO: Namespaces!
            => new() { KeySize = key.KeyBytes.Length, ValueSize = input.Length, RecordType = recordType };
        /// <inheritdoc/>
        public override RecordFieldInfo GetUpsertFieldInfo<TKey>(TKey key, ReadOnlySpan<byte> value, ref PinnedSpanByte input)
            // TODO: Namespaces!
            => new() { KeySize = key.KeyBytes.Length, ValueSize = value.Length, RecordType = recordType };
        /// <inheritdoc/>
        public override RecordFieldInfo GetUpsertFieldInfo<TKey>(TKey key, IHeapObject value, ref PinnedSpanByte input)
            // TODO: Namespaces!
            => new() { KeySize = key.KeyBytes.Length, ValueSize = ObjectIdMap.ObjectIdSize, ValueIsObject = true, RecordType = recordType };

        /// <inheritdoc />
        public override void ConvertOutputToHeap(ref PinnedSpanByte input, ref SpanByteAndMemory output)
        {
            // Currently the default is a no-op; the derived class inspects 'input' to decide whether to ConvertToHeap().
            //output.ConvertToHeap();
        }
    }
}