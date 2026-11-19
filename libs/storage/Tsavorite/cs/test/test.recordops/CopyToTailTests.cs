// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.IO;
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

        // A copy-on-read does not copy a source whose value slot holds a placeholder, which marks a released value. A read by key should not find
        // one, because it holds its bucket's shared lock from its lookup through the copy, so each test below puts a placeholder in a live
        // record's slot. The read still returns what it read, which the placeholder makes null or empty, but neither tail moves, and in a Debug
        // build the store reports the one copy it declined instead of failing its assertion.

        /// <summary>The key of the placeholder tests' only record.</summary>
        const int PlaceholderTestKey = 7;

#if DEBUG
        /// <summary>The copies the store declined because their source's value slot held a placeholder, counted through its test hook.</summary>
        private int placeholderCopiesDeclined;
#endif

        /// <summary>Object functions whose read completion accepts a released value, which reads as null.</summary>
        private sealed class PlaceholderObjectFunctions : TestObjectFunctions
        {
            public override void ReadCompletionCallback(ref DiskLogRecord srcLogRecord, ref TestObjectInput input, ref TestObjectOutput output, Empty ctx, Status status, RecordMetadata recordMetadata)
            {
            }
        }

        /// <summary>Have the store count the copies it declines for a placeholder source instead of failing its Debug assertion. Release builds
        /// have neither, so this does nothing there.</summary>
        private void ExpectPlaceholderSources()
        {
#if DEBUG
            placeholderCopiesDeclined = 0;
            store.testOnPlaceholderSourceNotCopied = () => placeholderCopiesDeclined++;
#endif
        }

        /// <summary>Upsert the placeholder tests' record, with an object value or an overflow value.</summary>
        private void UpsertPlaceholderTestRecord(ReadOnlySpan<byte> key, bool objectValue)
        {
            if (objectValue)
            {
                using var session = store.NewSession<TestSpanByteKey, TestObjectInput, TestObjectOutput, Empty, PlaceholderObjectFunctions>(new());
                ClassicAssert.IsFalse(session.BasicContext.Upsert(TestSpanByteKey.FromPinnedSpan(key), new TestObjectValue { value = PlaceholderTestKey }).IsPending);
                return;
            }

            using var spanSession = store.NewSession<TestSpanByteKey, PinnedSpanByte, SpanByteAndMemory, Empty, CTTSpanByteFunctions>(new CTTSpanByteFunctions(TestRecordType));
            Span<byte> valueBuf = stackalloc byte[MaxInlineValueBytes * 2];
            MakeValue(valueBuf, PlaceholderTestKey);
            ClassicAssert.IsFalse(spanSession.BasicContext.Upsert(TestSpanByteKey.FromPinnedSpan(key), valueBuf).IsPending);
        }

        /// <summary>Point the value slot of the record at the head of <paramref name="key"/>'s chain at a placeholder, as a release does, without
        /// changing the record's bytes.</summary>
        /// <returns>Whether that record is in the read cache.</returns>
        private bool PutPlaceholderInHeadRecord(ReadOnlySpan<byte> key)
        {
            ClassicAssert.IsTrue(store.FindHashBucketEntryForKey(TestSpanByteKey.FromPinnedSpan(key), out var entry), "The key is not in the index");
            var logRecord = (entry.IsReadCache ? store.readcacheBase : store.hlogBase)._wrapper.CreateLogRecord(entry.Address);
            ClassicAssert.IsTrue(logRecord.Key.SequenceEqual(key), "The record at the head of the chain is another key's");
            if (logRecord.DataHeader.ValueIsObject)
                ClassicAssert.IsNotNull(logRecord.ExchangeValueObjectSlot(SupersededObjectValue.Released), "The record held no object");
            else
                ClassicAssert.IsFalse(logRecord.ExchangeValueOverflowSlot(OverflowByteArray.Released).IsEmpty, "The record held no overflow array");
            return entry.IsReadCache;
        }

        /// <summary>Read the placeholder tests' key, copying it to <paramref name="readCopyTo"/> from wherever it is, and complete the read if it
        /// goes pending.</summary>
        /// <returns>The read's status; whether it went pending; and whether the value it read was released: an object that reads as null, or an
        /// overflow value that reads as empty.</returns>
        private (Status status, bool wentPending, bool readReleasedValue) ReadPlaceholderTestRecord(ReadOnlySpan<byte> key, bool objectValue, ReadCopyTo readCopyTo)
        {
            ReadOptions readOptions = new() { CopyOptions = new(ReadCopyFrom.AllImmutable, readCopyTo) };
            Status status;
            bool wentPending;
            if (objectValue)
            {
                using var session = store.NewSession<TestSpanByteKey, TestObjectInput, TestObjectOutput, Empty, PlaceholderObjectFunctions>(new());
                var bContext = session.BasicContext;
                TestObjectInput input = default;
                TestObjectOutput output = default;
                status = bContext.Read(TestSpanByteKey.FromPinnedSpan(key), ref input, ref output, ref readOptions, out _);
                wentPending = status.IsPending;
                if (wentPending)
                {
                    _ = bContext.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                    (status, output) = GetSinglePendingResult(completedOutputs);
                }
                return (status, wentPending, output.value is null);
            }

            using var spanSession = store.NewSession<TestSpanByteKey, PinnedSpanByte, SpanByteAndMemory, Empty, CTTSpanByteFunctions>(new CTTSpanByteFunctions(TestRecordType));
            var spanContext = spanSession.BasicContext;
            PinnedSpanByte spanInput = default;
            SpanByteAndMemory spanOutput = default;
            status = spanContext.Read(TestSpanByteKey.FromPinnedSpan(key), ref spanInput, ref spanOutput, ref readOptions, out _);
            wentPending = status.IsPending;
            if (wentPending)
            {
                _ = spanContext.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                (status, spanOutput) = GetSinglePendingResult(completedOutputs);
            }
            var readReleasedValue = spanOutput.Length == 0;
            spanOutput.Memory?.Dispose();
            return (status, wentPending, readReleasedValue);
        }

        /// <summary>Assert that a read of a source whose value slot holds a placeholder found the key and returned the released value, but copied
        /// nothing.</summary>
        private void AssertPlaceholderSourceNotCopied(Status status, bool readReleasedValue, long tailBefore, long readCacheTailBefore)
        {
            ClassicAssert.IsTrue(status.Found, $"Read not Found; {status}");
            ClassicAssert.IsTrue(readReleasedValue, "The read should return the released value, as null or empty");
            ClassicAssert.IsFalse(status.Record.Copied, $"A placeholder source was copied to the main log; {status}");
            ClassicAssert.IsFalse(status.Record.CopiedToReadCache, $"A placeholder source was copied to the read cache; {status}");
            ClassicAssert.AreEqual(tailBefore, store.Log.TailAddress, "The main-log tail moved");
            ClassicAssert.AreEqual(readCacheTailBefore, store.ReadCache?.TailAddress ?? 0, "The read-cache tail moved");
#if DEBUG
            ClassicAssert.AreEqual(1, placeholderCopiesDeclined, "The store should report exactly one declined copy");
#endif
        }

        /// <summary>A record in the immutable region of the main log, copied to the main log's tail or to the read cache.</summary>
        [Test]
        [Category(ReadTestCategory), Category(ReadCacheTestCategory)]
        public void ImmutablePlaceholderSourceIsNotCopied([Values(ReadCopyTo.MainLog, ReadCopyTo.ReadCache)] ReadCopyTo readCopyTo, [Values] bool objectValue)
        {
            CreateStore(readCopyTo);
            Span<byte> keyBuf = stackalloc byte[sizeof(int)];
            MakeKey(keyBuf, PlaceholderTestKey);

            UpsertPlaceholderTestRecord(keyBuf, objectValue);
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
            ClassicAssert.IsFalse(PutPlaceholderInHeadRecord(keyBuf), "The record should be in the main log");

            ExpectPlaceholderSources();
            var tailBefore = store.Log.TailAddress;
            var readCacheTailBefore = store.ReadCache?.TailAddress ?? 0;
            var (status, wentPending, readReleasedValue) = ReadPlaceholderTestRecord(keyBuf, objectValue, readCopyTo);
            ClassicAssert.IsFalse(wentPending, "An immutable record is read from memory");
            AssertPlaceholderSourceNotCopied(status, readReleasedValue, tailBefore, readCacheTailBefore);
        }

        /// <summary>A record read from disk, copied to the main log's tail or to the read cache. The placeholder goes into the record before it is
        /// flushed: the flush writes a released object as an empty value, which the read loads as the released placeholder. Only an object value
        /// is tested, because a disk image carries a released overflow value as an empty value, not as a placeholder.</summary>
        [Test]
        [Category(ReadTestCategory), Category(ReadCacheTestCategory)]
        public void DiskPlaceholderSourceIsNotCopied([Values(ReadCopyTo.MainLog, ReadCopyTo.ReadCache)] ReadCopyTo readCopyTo)
        {
            CreateStore(readCopyTo);
            Span<byte> keyBuf = stackalloc byte[sizeof(int)];
            MakeKey(keyBuf, PlaceholderTestKey);

            UpsertPlaceholderTestRecord(keyBuf, objectValue: true);
            ClassicAssert.IsFalse(PutPlaceholderInHeadRecord(keyBuf), "The record should be in the main log");
            store.Log.FlushAndEvict(wait: true);

            ExpectPlaceholderSources();
            var tailBefore = store.Log.TailAddress;
            var readCacheTailBefore = store.ReadCache?.TailAddress ?? 0;
            var (status, wentPending, readReleasedValue) = ReadPlaceholderTestRecord(keyBuf, objectValue: true, readCopyTo);
            ClassicAssert.IsTrue(wentPending, "An evicted record is read from disk");
            AssertPlaceholderSourceNotCopied(status, readReleasedValue, tailBefore, readCacheTailBefore);

            // Nothing was copied into memory, so the record is read from disk again.
            (status, wentPending, _) = ReadPlaceholderTestRecord(keyBuf, objectValue: true, ReadCopyTo.None);
            ClassicAssert.IsTrue(status.Found && wentPending, $"The record should still be read from disk; {status}");
        }

        /// <summary>A read-cache hit in the read cache's read-only region, which a read copies to the read cache's tail to give it a second
        /// chance.</summary>
        [Test]
        [Category(ReadTestCategory), Category(ReadCacheTestCategory)]
        public void ReadCachePlaceholderSourceIsNotPromoted([Values] bool objectValue)
        {
            CreateStore(ReadCopyTo.ReadCache);
            Span<byte> keyBuf = stackalloc byte[sizeof(int)];
            MakeKey(keyBuf, PlaceholderTestKey);

            // A read from disk copies the record to the read cache. Moving the read cache's ReadOnlyAddress past the copy makes a hit on it
            // eligible for promotion.
            UpsertPlaceholderTestRecord(keyBuf, objectValue);
            store.Log.FlushAndEvict(wait: true);
            var (status, wentPending, readReleasedValue) = ReadPlaceholderTestRecord(keyBuf, objectValue, ReadCopyTo.ReadCache);
            ClassicAssert.IsTrue(wentPending && status.Record.CopiedToReadCache && !readReleasedValue, $"The record should be copied to the read cache; {status}");
            store.ReadCache.ShiftReadOnlyAddress(store.ReadCache.TailAddress, wait: true);
            ClassicAssert.IsTrue(PutPlaceholderInHeadRecord(keyBuf), "The record should be in the read cache");

            ExpectPlaceholderSources();
            var tailBefore = store.Log.TailAddress;
            var readCacheTailBefore = store.ReadCache.TailAddress;
            (status, wentPending, readReleasedValue) = ReadPlaceholderTestRecord(keyBuf, objectValue, ReadCopyTo.ReadCache);
            ClassicAssert.IsFalse(wentPending, "A read-cache hit is read from memory");
            AssertPlaceholderSourceNotCopied(status, readReleasedValue, tailBefore, readCacheTailBefore);
        }
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