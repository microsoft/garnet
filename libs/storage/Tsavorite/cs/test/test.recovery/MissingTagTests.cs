// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test
{
    using ScanStoreFunctions = StoreFunctions<MissingTagTests.ScanKeyComparer, DefaultRecordTriggers>;

    /// <summary>
    /// Operations that meet a record whose key has no hash-index tag (FindTag returns false). No version of such a key is
    /// reachable, for example after a delete elided its tag chain, so the record must be skipped, not copied or reported.
    /// The tests remove the tag directly; see <see cref="RemoveTag{TAllocator}"/>.
    /// </summary>
    [TestFixture]
    internal class MissingTagTests : TestBase
    {
        /// <summary>Places every key in one tag chain unless <paramref name="distinct"/>, which hashes keys normally.</summary>
        internal readonly struct ScanKeyComparer(bool distinct) : IKeyComparer
        {
            /// <summary>Whether keys get their own hash chains; colliding keys can hold the hash slot of a deleted key.</summary>
            readonly bool distinct = distinct;

            public bool Equals<TFirstKey, TSecondKey>(TFirstKey first, TSecondKey second)
                where TFirstKey : IKey, allows ref struct
                where TSecondKey : IKey, allows ref struct
                => first.KeyBytes.SequenceEqual(second.KeyBytes);

            public long GetHashCode64<TKey>(TKey key) where TKey : IKey, allows ref struct
                => distinct ? SpanByteComparer.StaticGetHashCode64(key.KeyBytes) : 1;
        }

        /// <summary>Collects a lookup scan's records by key.</summary>
        sealed class CollectingScan : IScanIteratorFunctions
        {
            /// <summary>Keys and values actually delivered by the lookup scan.</summary>
            internal readonly Dictionary<long, long> records = [];

            public bool OnStart(long beginAddress, long endAddress) => true;
            public void OnStop(bool completed, long numberOfRecords) { }
            public void OnException(Exception exception, long numberOfRecords) { }
            public bool Reader<TSourceLogRecord>(in TSourceLogRecord record, RecordMetadata metadata, long numberOfRecords,
                out CursorRecordResult result) where TSourceLogRecord : ISourceLogRecord
            {
                records.Add(BitConverter.ToInt64(record.Key), BitConverter.ToInt64(record.ValueSpan));
                result = CursorRecordResult.Accept;
                return true;
            }
        }

        [SetUp]
        public void Setup() => DeleteDirectory(MethodTestDir, wait: true);

        [TearDown]
        public void TearDown() => OnTearDown();

        /// <summary>
        /// Compaction must skip a record whose key has no hash-index entry (its tag is absent). No lookup can reach that record, so copying it
        /// to the tail would make its key readable again; inserting the copy would also need the absent entry.
        /// </summary>
        /// <param name="compactionType">Scan or lookup compaction.</param>
        /// <param name="objectAllocator">Use the object allocator rather than the span-byte allocator.</param>
        /// <param name="evict">Evict the log before compacting, so that compaction reads its range from disk.</param>
        [Test]
        public void CompactionSkipsMissingTag([Values] CompactionType compactionType, [Values] bool objectAllocator, [Values] bool evict)
        {
            if (objectAllocator)
                CheckCompactionMissingTag<ObjectAllocator<ScanStoreFunctions>>((settings, functions) => new(settings, functions), compactionType, evict);
            else
                CheckCompactionMissingTag<SpanByteAllocator<ScanStoreFunctions>>((settings, functions) => new(settings, functions), compactionType, evict);
        }

        /// <summary>Runs <see cref="CompactionSkipsMissingTag"/> for one allocator.</summary>
        void CheckCompactionMissingTag<TAllocator>(Func<AllocatorSettings, ScanStoreFunctions, TAllocator> factory, CompactionType compactionType, bool evict)
            where TAllocator : IAllocator<ScanStoreFunctions>
        {
            // Each key has its own tag chain (distinct: true).
            using var log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "missing-tag.log"), deleteOnClose: true);
            using var objectLog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "missing-tag.obj.log"), deleteOnClose: true);
            using var store = new TsavoriteKV<ScanStoreFunctions, TAllocator>(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                ObjectLogDevice = objectLog,
                PageSize = MinKvLogPageSize,
                LogMemorySize = 1L << 20,
                MutableFraction = 1
            }, StoreFunctions.Create(new ScanKeyComparer(distinct: true)), factory);
            using var session = store.NewSession<TestSpanByteKey, long, long, Empty, SimpleLongSimpleFunctions>(new());
            var basic = session.BasicContext;

            // Key 1's record is the only record below compactUntil; key 2's lies above it, outside the compacted range. Compaction needs
            // its range read-only, so the log is flushed, and evicted when compaction must read the range from disk.
            var key = BitConverter.GetBytes(1L);
            _ = basic.Upsert(TestSpanByteKey.FromArray(key), BitConverter.GetBytes(10L));
            var compactUntil = store.Log.TailAddress;
            var survivingKey = BitConverter.GetBytes(2L);
            _ = basic.Upsert(TestSpanByteKey.FromArray(survivingKey), BitConverter.GetBytes(20L));
            if (evict)
                store.Log.FlushAndEvict(wait: true);
            else
                store.Log.Flush(wait: true);

            // Remove key 1's hash-index entry. Its record stays in the log, valid and not a tombstone, so compaction's scan supplies it.
            RemoveTag(store, key);
            using (var iter = store.Log.Scan(store.Log.BeginAddress, compactUntil))
            {
                Assert.That(iter.GetNext(), Is.True);
                Assert.That(iter.Info.Invalid || iter.Info.Tombstone, Is.False,
                    "The test needs a non-tombstone scan record whose tag is absent.");
            }

            // Compaction must finish the range without copying key 1's record to the tail or re-creating its entry.
            var tail = store.Log.TailAddress;
            Assert.That(session.Compact(compactUntil, compactionType), Is.EqualTo(compactUntil));
            Assert.That(store.Log.BeginAddress, Is.EqualTo(compactUntil));
            Assert.That(store.Log.TailAddress, Is.EqualTo(tail), "Compaction must not reinsert the unreachable source.");
            Assert.That(store.FindHashBucketEntryForKey(TestSpanByteKey.FromArray(key), out _), Is.False);
            long input = 0, output = 0;
            Assert.That(basic.Read(TestSpanByteKey.FromArray(key), ref input, ref output).Found, Is.False);

            // Key 2, outside the compacted range, keeps its value; reading it needs IO if the log was evicted.
            var status = basic.Read(TestSpanByteKey.FromArray(survivingKey), ref input, ref output);
            if (status.IsPending)
            {
                Assert.That(basic.CompletePendingWithOutputs(out var outputs, wait: true), Is.True);
                using (outputs)
                {
                    Assert.That(outputs.Next(), Is.True);
                    Assert.That(outputs.Current.Status.Found, Is.True);
                    Assert.That(outputs.Current.Output, Is.EqualTo(20L), "The live key outside the compacted range must be preserved.");
                }
            }
            else
            {
                Assert.That(status.Found, Is.True);
                Assert.That(output, Is.EqualTo(20L), "The live key outside the compacted range must be preserved.");
            }
        }

        /// <summary>
        /// A compaction copy whose search for a newer record of its key needs IO must finish without inserting when the key's hash-index
        /// entry disappears during that IO. The copy's saved source is then unreachable, as in <see cref="CompactionSkipsMissingTag"/>.
        /// </summary>
        /// <param name="objectAllocator">Use the object allocator rather than the span-byte allocator.</param>
        [Test]
        public void PendingCompactionSkipsMissingTag([Values] bool objectAllocator)
        {
            if (objectAllocator)
                CheckPendingCompactionMissingTag<ObjectAllocator<ScanStoreFunctions>>((settings, functions) => new(settings, functions));
            else
                CheckPendingCompactionMissingTag<SpanByteAllocator<ScanStoreFunctions>>((settings, functions) => new(settings, functions));
        }

        /// <summary>Runs <see cref="PendingCompactionSkipsMissingTag"/> for one allocator.</summary>
        void CheckPendingCompactionMissingTag<TAllocator>(Func<AllocatorSettings, ScanStoreFunctions, TAllocator> factory)
            where TAllocator : IAllocator<ScanStoreFunctions>
        {
            // Every key shares one tag chain (distinct: false).
            using var log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "pending-missing-tag.log"), deleteOnClose: true);
            using var objectLog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "pending-missing-tag.obj.log"), deleteOnClose: true);
            using var store = new TsavoriteKV<ScanStoreFunctions, TAllocator>(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                ObjectLogDevice = objectLog,
                PageSize = MinKvLogPageSize,
                LogMemorySize = 1L << 20,
                MutableFraction = 1
            }, StoreFunctions.Create(new ScanKeyComparer(distinct: false)), factory);
            using var session = store.NewSession<TestSpanByteKey, long, long, Empty, SimpleLongSimpleFunctions>(new());
            var basic = session.BasicContext;

            // The chain holds key 1's record, then key 2's at its head; both are evicted to disk.
            var key = BitConverter.GetBytes(1L);
            _ = basic.Upsert(TestSpanByteKey.FromArray(key), BitConverter.GetBytes(10L));
            var sourceEnd = store.Log.TailAddress;
            _ = basic.Upsert(TestSpanByteKey.FromArray(BitConverter.GetBytes(2L)), BitConverter.GetBytes(20L));
            store.Log.FlushAndEvict(wait: true);

            // Copying key 1's record first searches its chain for a newer record of key 1. The chain's head, key 2's record, is on disk,
            // so the copy goes pending.
            using var iter = store.Log.Scan(store.Log.BeginAddress, sourceEnd);
            Assert.That(iter.GetNext(), Is.True);
            var source = (ISourceLogRecord)iter;
            Assert.That(basic.CompactionCopyToTail(in source, iter.CurrentAddress, iter.NextAddress).IsPending, Is.True,
                "The colliding disk record must force the conditional copy to search through IO.");

            // Model the tag disappearing after the liveness search has gone pending, without changing its saved source. The continuation
            // must then complete without error, allocate nothing, and leave the entry absent.
            RemoveTag(store, key);
            var tail = store.Log.TailAddress;
            Assert.That(basic.CompletePendingWithOutputs(out var outputs, wait: true), Is.True);
            using (outputs)
            {
                Assert.That(outputs.Next(), Is.True, "The pending copy must finish successfully without inserting.");
                Assert.That(outputs.Current.Status.IsFaulted || outputs.Current.Status.IsCanceled, Is.False);
                Assert.That(outputs.Next(), Is.False);
            }
            Assert.That(store.Log.TailAddress, Is.EqualTo(tail), "The continuation must not allocate or reinsert its saved source.");
            Assert.That(store.FindHashBucketEntryForKey(TestSpanByteKey.FromArray(key), out _), Is.False);
        }

        /// <summary>
        /// A lookup scan must skip a valid, non-tombstone record whose key has no hash-index entry (its tag is absent): no version of the key
        /// is reachable, so reporting the record would return a key that does not exist. Bounded and unbounded lookups reach the same check.
        /// </summary>
        /// <param name="objectAllocator">Use the object allocator rather than the span-byte allocator.</param>
        /// <param name="evict">Flush and evict the log before scanning, so that the records are read from disk.</param>
        /// <param name="bounded">Bound the scan at the tail, as slot migration does, rather than leaving it unbounded, as Garnet's SCAN does.</param>
        [Test]
        public void LookupScanSkipsMissingTag([Values] bool objectAllocator, [Values] bool evict, [Values] bool bounded)
        {
            if (objectAllocator)
                CheckLookupScanMissingTag<ObjectAllocator<ScanStoreFunctions>>((settings, functions) => new(settings, functions), evict, bounded);
            else
                CheckLookupScanMissingTag<SpanByteAllocator<ScanStoreFunctions>>((settings, functions) => new(settings, functions), evict, bounded);
        }

        /// <summary>Runs <see cref="LookupScanSkipsMissingTag"/> for one allocator.</summary>
        void CheckLookupScanMissingTag<TAllocator>(Func<AllocatorSettings, ScanStoreFunctions, TAllocator> factory, bool evict, bool bounded)
            where TAllocator : IAllocator<ScanStoreFunctions>
        {
            // Each key has its own tag chain (distinct: true).
            using var log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "lookup-missing-tag.log"), deleteOnClose: true);
            using var objectLog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "lookup-missing-tag.obj.log"), deleteOnClose: true);
            using var store = new TsavoriteKV<ScanStoreFunctions, TAllocator>(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                ObjectLogDevice = objectLog,
                PageSize = MinKvLogPageSize,
                LogMemorySize = 1L << 20,
                MutableFraction = 1
            }, StoreFunctions.Create(new ScanKeyComparer(distinct: true)), factory);
            using var session = store.NewSession<TestSpanByteKey, long, long, Empty, SimpleLongSimpleFunctions>(new());
            var basic = session.BasicContext;

            // Key 1's record is the only record below sourceEnd; key 2's follows it.
            var key = BitConverter.GetBytes(1L);
            _ = basic.Upsert(TestSpanByteKey.FromArray(key), BitConverter.GetBytes(10L));
            var sourceEnd = store.Log.TailAddress;
            _ = basic.Upsert(TestSpanByteKey.FromArray(BitConverter.GetBytes(2L)), BitConverter.GetBytes(20L));
            if (evict)
                store.Log.FlushAndEvict(wait: true);

            // Remove key 1's hash-index entry. Its record stays in the log, valid and not a tombstone, so the scan supplies it.
            RemoveTag(store, key);
            using (var iter = store.Log.Scan(store.Log.BeginAddress, sourceEnd))
            {
                Assert.That(iter.GetNext(), Is.True);
                Assert.That(iter.Info.Invalid || iter.Info.Tombstone, Is.False,
                    "The test needs a valid, non-tombstone record whose tag is absent.");
            }

            // The scan must report only key 2.
            long cursor = 0;
            var scanner = new CollectingScan();
            var tail = store.Log.TailAddress;
            _ = session.ScanCursor(ref cursor, long.MaxValue, scanner, tail, maxAddress: bounded ? tail : long.MaxValue);
            Assert.That(scanner.records, Is.EquivalentTo(new Dictionary<long, long> { [2] = 20 }),
                "The scan must skip the unreachable record and report the live key.");
        }

        /// <summary>Removes the hash-index entry of <paramref name="key"/>'s tag, leaving its records in the log. When keys share one tag chain,
        /// this removes the entry for all of them.</summary>
        static void RemoveTag<TAllocator>(TsavoriteKV<ScanStoreFunctions, TAllocator> store, byte[] key)
            where TAllocator : IAllocator<ScanStoreFunctions>
        {
            store.epoch.Resume();
            try
            {
                HashEntryInfo hei = new(store.storeFunctions.GetKeyHashCode64(TestSpanByteKey.FromArray(key)));
                Assert.That(store.FindTag(ref hei), Is.True);
                Assert.That(hei.TryElide(), Is.True);
                Assert.That(store.FindTag(ref hei), Is.False);
            }
            finally
            {
                store.epoch.Suspend();
            }
        }
    }
}