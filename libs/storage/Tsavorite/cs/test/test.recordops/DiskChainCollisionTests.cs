// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.Objects
{
    using ClassAllocator = ObjectAllocator<StoreFunctions<CollidingKeyComparer, DiskRecordDisposalTriggers>>;
    using ClassStoreFunctions = StoreFunctions<CollidingKeyComparer, DiskRecordDisposalTriggers>;
    using CollisionContext = BasicContext<TestSpanByteKey, PinnedSpanByte, CollisionOutput, Empty, CollisionFunctions,
        StoreFunctions<CollidingKeyComparer, DiskRecordDisposalTriggers>, ObjectAllocator<StoreFunctions<CollidingKeyComparer, DiskRecordDisposalTriggers>>>;

    /// <summary>Full-key equality with a constant hash, so every key shares one hash bucket and tag.</summary>
    internal readonly struct CollidingKeyComparer : IKeyComparer
    {
        public bool Equals<TFirstKey, TSecondKey>(TFirstKey k1, TSecondKey k2)
            where TFirstKey : IKey, allows ref struct
            where TSecondKey : IKey, allows ref struct
            => SpanByteComparer.StaticEquals(k1.KeyBytes, k2.KeyBytes);

        public long GetHashCode64<TKey>(TKey key)
            where TKey : IKey, allows ref struct
            => 42;
    }

    /// <summary>The value read by <see cref="CollisionFunctions"/>: the value bytes, or the bytes of a <see cref="TestObjectValue"/>'s int.</summary>
    internal struct CollisionOutput
    {
        internal byte[] value;
    }

    /// <summary>
    /// Reads span and <see cref="TestObjectValue"/> values, creates span values by RMW, and records the key of the disk record
    /// passed to a completion callback, which must be the requested key's record, or no record if the key was not found.
    /// </summary>
    internal sealed class CollisionFunctions : SessionFunctionsBase<PinnedSpanByte, CollisionOutput, Empty>
    {
        internal int completionCount;
        internal byte[] completionRecordKey;

        public override bool Reader<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref PinnedSpanByte input, ref CollisionOutput output, ref ReadInfo readInfo)
        {
            output.value = srcLogRecord.DataHeader.ValueIsObject
                ? BitConverter.GetBytes(((TestObjectValue)srcLogRecord.ValueObject).value)
                : srcLogRecord.ValueSpan.ToArray();
            return true;
        }

        public override bool InitialUpdater(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref PinnedSpanByte input, ref CollisionOutput output, ref RMWInfo rmwInfo)
            => dstLogRecord.TrySetValueSpanAndPrepareOptionals(input.ReadOnlySpan, in sizeInfo);

        public override RecordFieldInfo GetRMWInitialFieldInfo<TKey>(TKey key, ref PinnedSpanByte input)
            => new() { KeySize = key.KeyBytes.Length, ValueSize = input.Length };

        public override void ReadCompletionCallback(ref DiskLogRecord diskLogRecord, ref PinnedSpanByte input, ref CollisionOutput output, Empty ctx, Status status, RecordMetadata recordMetadata)
            => OnCompletion(ref diskLogRecord);

        public override void RMWCompletionCallback(ref DiskLogRecord diskLogRecord, ref PinnedSpanByte input, ref CollisionOutput output, Empty ctx, Status status, RecordMetadata recordMetadata)
            => OnCompletion(ref diskLogRecord);

        private void OnCompletion(ref DiskLogRecord diskLogRecord)
        {
            completionCount++;
            completionRecordKey = diskLogRecord.IsSet ? diskLogRecord.Key.ToArray() : null;
        }
    }

    /// <summary>Records the keys pushed by IterateKeyVersions and any exception reported to <see cref="OnException"/>.</summary>
    internal sealed class KeyVersionScanFunctions : IScanIteratorFunctions
    {
        internal readonly List<byte[]> keys = [];
        internal Exception exception;

        public bool OnStart(long beginAddress, long endAddress) => true;

        public bool Reader<TSourceLogRecord>(in TSourceLogRecord logRecord, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
            where TSourceLogRecord : ISourceLogRecord
        {
            cursorRecordResult = CursorRecordResult.Accept;
            keys.Add(logRecord.Key.ToArray());
            return true;
        }

        public void OnStop(bool completed, long numberOfRecords) { }

        public void OnException(Exception exception, long numberOfRecords) => this.exception = exception;
    }

    /// <summary>The <see cref="DiskLogRecord"/>s passed to OnDisposeDiskRecord: their number, and the values of those with an object value.</summary>
    internal sealed class DiskRecordDisposals
    {
        internal int recordCount;
        internal readonly List<int> objectValues = [];

        internal void Clear()
        {
            recordCount = 0;
            objectValues.Clear();
        }
    }

    /// <summary>Records each <see cref="DiskLogRecord"/> passed to OnDisposeDiskRecord in a <see cref="DiskRecordDisposals"/>.</summary>
    internal readonly struct DiskRecordDisposalTriggers : IRecordTriggers
    {
        private readonly DiskRecordDisposals disposals;

        internal DiskRecordDisposalTriggers(DiskRecordDisposals disposals) => this.disposals = disposals;

        public void OnDisposeDiskRecord(ref DiskLogRecord logRecord, DisposeReason reason)
        {
            // Some callers pass an unset record when there is nothing to dispose.
            if (disposals is null || !logRecord.IsSet)
                return;
            disposals.recordCount++;
            if (logRecord.DataHeader.ValueIsObject)
                disposals.objectValues.Add(((TestObjectValue)logRecord.ValueObject).value);
        }
    }

    /// <summary>
    /// Pending disk reads whose hash chain contains records for other keys in the same bucket and tag. When an on-disk record has an
    /// Overflow key, the key is compared only after it is read from the object log, so a mismatch is detected after the record has
    /// been taken from the IO buffer; the read must then continue to the record's PreviousAddress, or end at the start of the chain.
    /// </summary>
    [TestFixture]
    internal class DiskChainCollisionTests : TestBase
    {
        const int MaxInlineKeyBytes = 16;
        const int MaxInlineValueBytes = 32;
        const int InlineKeyBytes = 8;
        const int OverflowKeyBytes = MaxInlineKeyBytes * 4;
        const int InlineValueBytes = 8;
        const int OverflowValueBytes = MaxInlineValueBytes * 4;

        public enum ValueKind { Inline, Overflow, Object }

        private TsavoriteKV<ClassStoreFunctions, ClassAllocator> store;
        private IDevice log, objlog;
        private DiskRecordDisposals disposals;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "test.log"), deleteOnClose: true);
            objlog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "test.obj.log"), deleteOnClose: true);
            disposals = new DiskRecordDisposals();

            store = new(new KVSettings
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                ObjectLogDevice = objlog,
                MutableFraction = 0.1,
                LogMemorySize = 1L << 20,
                PageSize = MinKvLogPageSize,
                MaxInlineKeySize = MaxInlineKeyBytes,
                MaxInlineValueSize = MaxInlineValueBytes,
            }, StoreFunctions.Create(new CollidingKeyComparer(), () => new TestObjectValue.Serializer(), new DiskRecordDisposalTriggers(disposals)),
                (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));
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

        private static byte[] MakeKey(int id, bool overflow)
        {
            var key = new byte[overflow ? OverflowKeyBytes : InlineKeyBytes];
            key.AsSpan().Fill((byte)id);
            return key;
        }

        /// <summary>The value bytes for <paramref name="id"/>, as returned by <see cref="CollisionFunctions.Reader"/>.</summary>
        private static byte[] MakeValue(int id, ValueKind valueKind)
        {
            if (valueKind == ValueKind.Object)
                return BitConverter.GetBytes(id * 1000);
            var value = new byte[valueKind == ValueKind.Overflow ? OverflowValueBytes : InlineValueBytes];
            value.AsSpan().Fill((byte)(id + 100));
            return value;
        }

        private static void Upsert(CollisionContext bContext, byte[] key, int id, ValueKind valueKind)
        {
            var status = valueKind == ValueKind.Object
                ? bContext.Upsert(TestSpanByteKey.FromArray(key), new TestObjectValue { value = id * 1000 })
                : bContext.Upsert(TestSpanByteKey.FromArray(key), MakeValue(id, valueKind));
            ClassicAssert.IsFalse(status.IsPending, $"Upsert should not go pending; {status}");
        }

        /// <summary>Reads <paramref name="key"/>, which must go pending, and returns the completed result.</summary>
        private static (Status status, CollisionOutput output) ReadPending(CollisionContext bContext, byte[] key)
        {
            PinnedSpanByte input = default;
            CollisionOutput output = default;
            var status = bContext.Read(TestSpanByteKey.FromArray(key), ref input, ref output);
            ClassicAssert.IsTrue(status.IsPending, $"Expected a pending read from disk; {status}");
            _ = bContext.CompletePendingWithOutputs(out var completedOutputs, wait: true);
            return GetSinglePendingResult(completedOutputs);
        }

        /// <summary>Reads <paramref name="key"/> from disk and verifies its value, and that the completion callback received its record.</summary>
        private static void ReadFromDiskAndVerify(CollisionContext bContext, CollisionFunctions functions, byte[] key, byte[] expectedValue)
        {
            var (status, output) = ReadPending(bContext, key);
            ClassicAssert.IsTrue(status.Found, $"Expected Found; {status}");
            CollectionAssert.AreEqual(expectedValue, output.value, "Value mismatch");
            CollectionAssert.AreEqual(key, functions.completionRecordKey, "ReadCompletionCallback received a record with the wrong key");
        }

        [Test]
        [Category(TsavoriteKVTestCategory), Category(ReadTestCategory), Category(OverflowFieldCategory)]
        public void ReadMissingKeyCollidingWithDiskOverflowKey([Values] ValueKind valueKind, [Values] bool requestKeyIsOverflow)
        {
            var functions = new CollisionFunctions();
            using var session = store.NewSession<TestSpanByteKey, PinnedSpanByte, CollisionOutput, Empty, CollisionFunctions>(functions);
            var bContext = session.BasicContext;

            var keyA = MakeKey(1, overflow: true);
            var keyB = MakeKey(2, requestKeyIsOverflow);
            Upsert(bContext, keyA, 1, valueKind);
            store.Log.FlushAndEvict(wait: true);

            // The read of B goes pending to A's record, whose Overflow key does not match; A is the only record in the chain.
            var (status, _) = ReadPending(bContext, keyB);
            ClassicAssert.IsTrue(status.NotFound, $"Expected NotFound; {status}");
            ClassicAssert.AreEqual(1, functions.completionCount, "Expected one ReadCompletionCallback");
            ClassicAssert.IsNull(functions.completionRecordKey, "ReadCompletionCallback received another key's record");

            ReadFromDiskAndVerify(bContext, functions, keyA, MakeValue(1, valueKind));
        }

        [Test]
        [Category(TsavoriteKVTestCategory), Category(ReadTestCategory), Category(OverflowFieldCategory)]
        public void ReadKeyBehindCollidingDiskOverflowKey([Values] ValueKind valueKind)
        {
            var functions = new CollisionFunctions();
            using var session = store.NewSession<TestSpanByteKey, PinnedSpanByte, CollisionOutput, Empty, CollisionFunctions>(functions);
            var bContext = session.BasicContext;

            // B is inserted first, so A precedes it in the hash chain.
            var keyB = MakeKey(2, overflow: true);
            var keyA = MakeKey(1, overflow: true);
            Upsert(bContext, keyB, 2, valueKind);
            Upsert(bContext, keyA, 1, valueKind);
            store.Log.FlushAndEvict(wait: true);

            // The read of B first reads A, whose Overflow key does not match, then continues to A's PreviousAddress, which is B.
            ReadFromDiskAndVerify(bContext, functions, keyB, MakeValue(2, valueKind));
            ReadFromDiskAndVerify(bContext, functions, keyA, MakeValue(1, valueKind));
        }

        [Test]
        [Category(TsavoriteKVTestCategory), Category(ReadTestCategory), Category(OverflowFieldCategory)]
        public void OnDisposeDiskRecordReceivesMismatchedOverflowKeyRecord([Values] ValueKind valueKind, [Values] bool keyExists)
        {
            using var session = store.NewSession<TestSpanByteKey, PinnedSpanByte, CollisionOutput, Empty, CollisionFunctions>(new CollisionFunctions());
            var bContext = session.BasicContext;

            // If B exists it is inserted first, so A precedes it in the hash chain.
            var keyA = MakeKey(1, overflow: true);
            var keyB = MakeKey(2, overflow: true);
            if (keyExists)
                Upsert(bContext, keyB, 2, valueKind);
            Upsert(bContext, keyA, 1, valueKind);
            store.Log.FlushAndEvict(wait: true);
            disposals.Clear();

            // A's record is passed to OnDisposeDiskRecord whether the chain continues to B or ends at A, then B's record if it exists.
            // A's value is not read, so OnDisposeDiskRecord must not see a value object for it.
            var (status, _) = ReadPending(bContext, keyB);
            ClassicAssert.AreEqual(keyExists, status.Found, $"Unexpected status; {status}");
            ClassicAssert.AreEqual(keyExists ? 2 : 1, disposals.recordCount, "Unexpected number of records passed to OnDisposeDiskRecord");
            CollectionAssert.AreEqual(keyExists && valueKind == ValueKind.Object ? [2 * 1000] : Array.Empty<int>(), disposals.objectValues,
                "Unexpected object values passed to OnDisposeDiskRecord");
        }

        [Test]
        [Category(TsavoriteKVTestCategory), Category(RMWTestCategory), Category(OverflowFieldCategory)]
        public void RMWMissingKeyCollidingWithDiskOverflowKey([Values] ValueKind valueKind)
        {
            var functions = new CollisionFunctions();
            using var session = store.NewSession<TestSpanByteKey, PinnedSpanByte, CollisionOutput, Empty, CollisionFunctions>(functions);
            var bContext = session.BasicContext;

            var keyA = MakeKey(1, overflow: true);
            var keyB = MakeKey(2, overflow: true);
            Upsert(bContext, keyA, 1, valueKind);
            store.Log.FlushAndEvict(wait: true);

            // The RMW of B goes pending to A's record, whose Overflow key does not match, so B is created by InitialUpdater.
            var rmwValue = GC.AllocateArray<byte>(InlineValueBytes, pinned: true);
            rmwValue.AsSpan().Fill(2);
            var input = PinnedSpanByte.FromPinnedSpan(rmwValue);
            CollisionOutput output = default;
            var status = bContext.RMW(TestSpanByteKey.FromArray(keyB), ref input, ref output);
            ClassicAssert.IsTrue(status.IsPending, $"Expected a pending RMW; {status}");
            _ = bContext.CompletePendingWithOutputs(out var completedOutputs, wait: true);
            (status, _) = GetSinglePendingResult(completedOutputs);
            ClassicAssert.IsTrue(status.NotFound && status.Record.Created, $"Expected NotFound with a created record; {status}");
            ClassicAssert.AreEqual(1, functions.completionCount, "Expected one RMWCompletionCallback");
            ClassicAssert.IsNull(functions.completionRecordKey, "RMWCompletionCallback received another key's record");

            // B is now in memory; A is still read from disk.
            PinnedSpanByte readInput = default;
            output = default;
            status = bContext.Read(TestSpanByteKey.FromArray(keyB), ref readInput, ref output);
            ClassicAssert.IsTrue(status.Found && !status.IsPending, $"Expected an in-memory Found; {status}");
            CollectionAssert.AreEqual(rmwValue, output.value, "Value mismatch for the RMW-created record");

            ReadFromDiskAndVerify(bContext, functions, keyA, MakeValue(1, valueKind));
        }

        [Test]
        [Category(TsavoriteKVTestCategory), Category(IteratorCategory)]
        public void IterateKeyVersionsPastCollidingDiskKeys([Values] bool overflowKeys, [Values] bool keyExists)
        {
            using var session = store.NewSession<TestSpanByteKey, PinnedSpanByte, CollisionOutput, Empty, CollisionFunctions>(new CollisionFunctions());
            var bContext = session.BasicContext;

            // The hash chain is A -> B -> C (or A -> C if B does not exist).
            var keyA = MakeKey(1, overflowKeys);
            var keyB = MakeKey(2, overflowKeys);
            var keyC = MakeKey(3, overflowKeys);
            Upsert(bContext, keyC, 3, ValueKind.Inline);
            if (keyExists)
                Upsert(bContext, keyB, 2, ValueKind.Inline);
            Upsert(bContext, keyA, 1, ValueKind.Inline);
            store.Log.FlushAndEvict(wait: true);

            // The on-disk walk for B passes A, pushes B if it exists, and then reaches the end of the chain at C without another match.
            var scanFunctions = new KeyVersionScanFunctions();
            ClassicAssert.IsTrue(store.Log.IterateKeyVersions(ref scanFunctions, TestSpanByteKey.FromArray(keyB)));
            ClassicAssert.IsNull(scanFunctions.exception, $"Unexpected exception: {scanFunctions.exception}");
            ClassicAssert.AreEqual(keyExists ? 1 : 0, scanFunctions.keys.Count, "Unexpected number of key versions");
            if (keyExists)
                CollectionAssert.AreEqual(keyB, scanFunctions.keys[0], "IterateKeyVersions pushed a record with the wrong key");
        }
    }
}