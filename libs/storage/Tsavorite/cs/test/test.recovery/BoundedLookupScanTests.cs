// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using NUnit.Framework;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test
{
    using ScanStoreFunctions = StoreFunctions<BoundedLookupScanTests.ScanKeyComparer, DefaultRecordTriggers>;

    /// <summary>
    /// Tests that bounded lookup scans and streaming checkpoints report each key's right version when records in their range have been
    /// elided, sealed or invalidated.
    /// </summary>
    /// <remarks>
    /// Terms used in these tests:
    /// <list type="bullet">
    /// <item>A <b>lookup scan</b> reports a record only if no newer record of its key lies between it and the scan's bound, its maxAddress.
    /// A <c>ScanCursor</c> with a maxAddress, <c>IterateLookupSnapshot</c>, and streaming checkpoint phase 2 are bounded lookup scans.</item>
    /// <item>An update that does not change a record in place copies it to the tail and <b>seals</b> the source, which stays valid. If the
    /// source was the only record in its tag chain and no checkpoint freezes it, the update instead <b>elides</b> it: it removes the source
    /// from the chain and marks it sealed and invalid. An in-place delete of a chain's only record makes it a tombstone, then elides it,
    /// removing the chain's hash-index entry.</item>
    /// <item><see cref="CopyOnlyFunctions"/> makes every upsert copy, and <see cref="ScanKeyComparer"/> can put every key in one tag chain, so
    /// the tests choose which records are elided and which keys share a hash-index entry.</item>
    /// </list>
    /// </remarks>
    [TestFixture]
    internal class BoundedLookupScanTests : TestBase
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
        /// <param name="onStart">Runs from <see cref="OnStart"/>. <c>IterateLookupSnapshot</c> calls that after sampling its bound, so
        /// writes made here land past the bound.</param>
        sealed class CollectingScan(Action onStart = null) : IScanIteratorFunctions
        {
            /// <summary>Keys and values actually delivered by the lookup scan.</summary>
            internal readonly Dictionary<long, long> records = [];

            public bool OnStart(long beginAddress, long endAddress)
            {
                onStart?.Invoke();
                return true;
            }
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

        /// <summary>Collects a streaming checkpoint's records by key.</summary>
        /// <param name="firstRecord">Runs once, from the callback for the first record streamed. The tests that pass it keep the read-only
        /// region empty, so phase 1 streams nothing, and write their first key before any other. It therefore runs during phase 2, after
        /// phase 2 sampled its bound, and before phase 2 reaches any later key's records.</param>
        sealed class CollectingStream(Action firstRecord = null) : IStreamingSnapshotIteratorFunctions
        {
            /// <summary>Distinct streamed keys, including replacements delivered by either scan phase.</summary>
            internal readonly Dictionary<long, long> records = [];

            /// <summary>Whether <c>firstRecord</c> has already run.</summary>
            bool invoked;

            public bool OnStart(Guid token, long currentVersion, long nextVersion) => true;
            public void OnStop(bool completed, long numberOfRecords) { }
            public void OnException(Exception exception, long numberOfRecords) { }
            public bool Reader<TSourceLogRecord>(in TSourceLogRecord record, RecordMetadata metadata, long numberOfRecords)
                where TSourceLogRecord : ISourceLogRecord
            {
                records[BitConverter.ToInt64(record.Key)] = BitConverter.ToInt64(record.ValueSpan);
                if (!invoked)
                {
                    invoked = true;
                    firstRecord?.Invoke();
                }
                return true;
            }
        }

        /// <summary>Upsert functions that never write in place, so every update of an existing key copies it to the tail and seals or
        /// elides the source.</summary>
        sealed class CopyOnlyFunctions : SpanByteFunctions<Empty>
        {
            public override bool InPlaceWriter(ref LogRecord logRecord, ref PinnedSpanByte input, ReadOnlySpan<byte> src,
                ref SpanByteAndMemory output, ref UpsertInfo upsertInfo) => false;
        }

        [SetUp]
        public void Setup() => DeleteDirectory(MethodTestDir, wait: true);

        [TearDown]
        public void TearDown() => OnTearDown();

        /// <summary>
        /// A bounded lookup scan and a streaming checkpoint must not report a deleted key's elided records once another key holds the deleted
        /// key's hash-index entry. Following that entry finds no newer record of the deleted key in the scan's range, and the deleted key has
        /// no record past the bound, so the scan must skip them; reporting them would return a deleted key, and fields its elision released.
        /// </summary>
        /// <param name="objectAllocator">Use the object allocator rather than the span-byte allocator.</param>
        /// <param name="largeKey">Use a key longer than eight bytes. The object allocator keeps its 2048-byte key out of line, in an overflow
        /// allocation that elision releases; the span-byte allocator keeps its 256-byte key inline.</param>
        /// <param name="evict">Flush and evict the log before scanning, so that the records are read from disk.</param>
        [Test]
        public void BoundedLookupSkipsElidedRecords([Values] bool objectAllocator, [Values] bool largeKey, [Values] bool evict)
        {
            if (objectAllocator)
                CheckElidedRecords<ObjectAllocator<ScanStoreFunctions>>((settings, functions) => new(settings, functions), largeKey ? 2048 : sizeof(long), evict);
            else
                CheckElidedRecords<SpanByteAllocator<ScanStoreFunctions>>((settings, functions) => new(settings, functions), largeKey ? 256 : sizeof(long), evict);
        }

        /// <summary>Runs <see cref="BoundedLookupSkipsElidedRecords"/> for one allocator.</summary>
        void CheckElidedRecords<TAllocator>(Func<AllocatorSettings, ScanStoreFunctions, TAllocator> factory, int keyLength, bool evict)
            where TAllocator : IAllocator<ScanStoreFunctions>
        {
            // Every key shares one tag chain (distinct: false), so all keys use the same hash-index entry.
            using var log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "scan.log"), deleteOnClose: true);
            using var objectLog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "scan.obj.log"), deleteOnClose: true);
            using var store = new TsavoriteKV<ScanStoreFunctions, TAllocator>(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                ObjectLogDevice = objectLog,
                PageSize = MinKvLogPageSize,
                LogMemorySize = 1L << 20,
                MutableFraction = 1,
                CheckpointDir = MethodTestDir
            }, StoreFunctions.Create(new ScanKeyComparer(distinct: false)), factory);
            using var session = store.NewSession<TestSpanByteKey, PinnedSpanByte, SpanByteAndMemory, Empty, CopyOnlyFunctions>(new());
            var basic = session.BasicContext;
            var key = new byte[keyLength];
            BitConverter.TryWriteBytes(key, 1L);

            // Key 1 is written twice. The second write copies the key's only record, eliding the 10-record. The delete makes the 11-record
            // a tombstone and elides it too, removing the chain's hash-index entry; key 2's insert then creates that entry again.
            var originalAddress = store.Log.TailAddress;
            _ = basic.Upsert(TestSpanByteKey.FromArray(key), BitConverter.GetBytes(10L));
            _ = basic.Upsert(TestSpanByteKey.FromArray(key), BitConverter.GetBytes(11L));
            _ = basic.Delete(TestSpanByteKey.FromArray(key));
            var survivingKey = BitConverter.GetBytes(2L);
            _ = basic.Upsert(TestSpanByteKey.FromArray(survivingKey), BitConverter.GetBytes(20L));

            Assert.That(store.hlogBase._wrapper.CreateLogRecord(originalAddress).Info.Invalid, Is.True,
                "The old non-tombstone record must actually have been elided.");
            if (evict)
                store.Log.FlushAndEvict(wait: true);

            // A scan bounded at the tail must report only key 2.
            long cursor = 0;
            var scanner = new CollectingScan();
            _ = session.ScanCursor(ref cursor, long.MaxValue, scanner, store.Log.TailAddress, maxAddress: store.Log.TailAddress);
            Assert.That(scanner.records, Is.EquivalentTo(new Dictionary<long, long> { [2] = 20 }));

            // So must a streaming checkpoint: phase 1 streams the log if it was evicted, as the read-only region then covers it all, and
            // phase 2 streams it otherwise.
            var stream = new CollectingStream();
            _ = store.TakeFullCheckpointAsync(CheckpointType.StreamingSnapshot, streamingSnapshotIteratorFunctions: stream)
                .AsTask().WaitAsync(TimeSpan.FromSeconds(15)).GetAwaiter().GetResult();
            Assert.That(stream.records, Is.EquivalentTo(scanner.records));
        }

        /// <summary>
        /// A bounded lookup scan must report a key's latest record at the bound even after an update past the bound sealed it. Older records
        /// of its tag chain precede that record, so the update seals it but does not elide it, and it stays valid.
        /// </summary>
        /// <param name="objectAllocator">Use the object allocator rather than the span-byte allocator.</param>
        /// <param name="evict">Flush and evict the log before scanning, so that the records are read from disk.</param>
        [Test]
        public void BoundedLookupRetainsSealedValidVersion([Values] bool objectAllocator, [Values] bool evict)
        {
            if (objectAllocator)
                CheckSealedVersion<ObjectAllocator<ScanStoreFunctions>>((settings, functions) => new(settings, functions), evict);
            else
                CheckSealedVersion<SpanByteAllocator<ScanStoreFunctions>>((settings, functions) => new(settings, functions), evict);
        }

        /// <summary>Runs <see cref="BoundedLookupRetainsSealedValidVersion"/> for one allocator.</summary>
        void CheckSealedVersion<TAllocator>(Func<AllocatorSettings, ScanStoreFunctions, TAllocator> factory, bool evict)
            where TAllocator : IAllocator<ScanStoreFunctions>
        {
            // Every key shares one tag chain (distinct: false).
            using var log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "bounded.log"), deleteOnClose: true);
            using var objectLog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "bounded.obj.log"), deleteOnClose: true);
            using var store = new TsavoriteKV<ScanStoreFunctions, TAllocator>(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                ObjectLogDevice = objectLog,
                PageSize = MinKvLogPageSize,
                LogMemorySize = 1L << 20,
                MutableFraction = 1
            }, StoreFunctions.Create(new ScanKeyComparer(distinct: false)), factory);
            using var session = store.NewSession<TestSpanByteKey, PinnedSpanByte, SpanByteAndMemory, Empty, CopyOnlyFunctions>(new());
            var basic = session.BasicContext;
            var otherKey = BitConverter.GetBytes(2L);
            var key = BitConverter.GetBytes(1L);

            // Key 2's record and key 1's 9-record come first in the chain, so each later update of key 1 seals its source but does not
            // elide it. The 10-record is key 1's latest record at the bound.
            _ = basic.Upsert(TestSpanByteKey.FromArray(otherKey), BitConverter.GetBytes(20L));
            _ = basic.Upsert(TestSpanByteKey.FromArray(key), BitConverter.GetBytes(9L));
            var originalAddress = store.Log.TailAddress;
            _ = basic.Upsert(TestSpanByteKey.FromArray(key), BitConverter.GetBytes(10L));
            var bound = store.Log.TailAddress;

            // Past the bound, an update copies key 1 again and seals the 10-record, which must stay valid.
            _ = basic.Upsert(TestSpanByteKey.FromArray(key), new byte[256]);
            var original = store.hlogBase._wrapper.CreateLogRecord(originalAddress);
            Assert.That(original.Info.Valid && original.Info.SkipOnScan, Is.True);
            if (evict)
                store.Log.FlushAndEvict(wait: true);

            // At the bound, key 1's value is 10 and key 2's is 20.
            long cursor = 0;
            var scanner = new CollectingScan();
            _ = session.ScanCursor(ref cursor, long.MaxValue, scanner, bound, maxAddress: bound);
            Assert.That(scanner.records, Is.EquivalentTo(new Dictionary<long, long> { [1] = 10, [2] = 20 }));
        }

        /// <summary>
        /// A bounded lookup scan must report a key whose only record in its range was elided, by an update made after the scan sampled its
        /// bound. The key existed at the bound and still exists past it; reporting it is what slot migration and <c>IterateLookupSnapshot</c>'s
        /// callers, such as KEYS, need.
        /// </summary>
        /// <param name="objectAllocator">Use the object allocator rather than the span-byte allocator.</param>
        /// <param name="snapshot">Scan with <c>IterateLookupSnapshot</c>, which samples its own bound, rather than with a bounded
        /// <c>ScanCursor</c>.</param>
        /// <param name="evict">Flush and evict the log after the update, so that the scan reads the elided record from disk.</param>
        [Test]
        public void BoundedLookupReportsKeyElidedAfterBound([Values] bool objectAllocator, [Values] bool snapshot, [Values] bool evict)
        {
            if (objectAllocator)
                CheckKeyElidedAfterBound<ObjectAllocator<ScanStoreFunctions>>((settings, functions) => new(settings, functions), snapshot, evict);
            else
                CheckKeyElidedAfterBound<SpanByteAllocator<ScanStoreFunctions>>((settings, functions) => new(settings, functions), snapshot, evict);
        }

        /// <summary>Runs <see cref="BoundedLookupReportsKeyElidedAfterBound"/> for one allocator.</summary>
        void CheckKeyElidedAfterBound<TAllocator>(Func<AllocatorSettings, ScanStoreFunctions, TAllocator> factory, bool snapshot, bool evict)
            where TAllocator : IAllocator<ScanStoreFunctions>
        {
            using var log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "elided.log"), deleteOnClose: true);
            using var objectLog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "elided.obj.log"), deleteOnClose: true);
            using var store = new TsavoriteKV<ScanStoreFunctions, TAllocator>(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                ObjectLogDevice = objectLog,
                PageSize = MinKvLogPageSize,
                LogMemorySize = 1L << 20,
                MutableFraction = 1
            }, StoreFunctions.Create(new ScanKeyComparer(distinct: false)), factory);
            using var session = store.NewSession<TestSpanByteKey, PinnedSpanByte, SpanByteAndMemory, Empty, CopyOnlyFunctions>(new());

            // Key 1's 10-record is the only record in the log, and so the only record of its tag chain.
            var key = BitConverter.GetBytes(1L);
            var sourceAddress = store.Log.TailAddress;
            _ = session.BasicContext.Upsert(TestSpanByteKey.FromArray(key), BitConverter.GetBytes(10L));

            // Runs after the scan samples its bound: copies the key above the bound and elides its only in-range record.
            void UpdateAfterBound()
            {
                _ = session.BasicContext.Upsert(TestSpanByteKey.FromArray(key), BitConverter.GetBytes(11L));
                var source = store.hlogBase._wrapper.CreateLogRecord(sourceAddress).Info;
                Assert.That(source.Invalid && source.IsSealed, Is.True, "The update must elide the in-range record.");
                if (evict)
                    store.Log.FlushAndEvict(wait: true);
            }

            // IterateLookupSnapshot samples its bound before calling OnStart; a ScanCursor's bound is sampled here before the update.
            var scanner = new CollectingScan(snapshot ? UpdateAfterBound : null);
            if (snapshot)
                _ = session.IterateLookupSnapshot(ref scanner);
            else
            {
                var bound = store.Log.TailAddress;
                UpdateAfterBound();
                long cursor = 0;
                _ = session.ScanCursor(ref cursor, long.MaxValue, scanner, bound, maxAddress: bound);
            }

            // Only the key is checked: these callers use the reported key, not the elided record's value.
            Assert.That(scanner.records.Keys, Is.EquivalentTo([1L]), "A key live past the bound must be reported.");
        }

        /// <summary>
        /// A bounded lookup scan must not report an invalid record that is not sealed, even when its key has a record past the bound. Such a
        /// record is not a version of its key: recovery, for example, leaves a record it discards in this state.
        /// </summary>
        /// <param name="objectAllocator">Use the object allocator rather than the span-byte allocator.</param>
        [Test]
        public void BoundedLookupSkipsUnsealedInvalidRecord([Values] bool objectAllocator)
        {
            if (objectAllocator)
                CheckUnsealedInvalidRecord<ObjectAllocator<ScanStoreFunctions>>((settings, functions) => new(settings, functions));
            else
                CheckUnsealedInvalidRecord<SpanByteAllocator<ScanStoreFunctions>>((settings, functions) => new(settings, functions));
        }

        /// <summary>Runs <see cref="BoundedLookupSkipsUnsealedInvalidRecord"/> for one allocator.</summary>
        void CheckUnsealedInvalidRecord<TAllocator>(Func<AllocatorSettings, ScanStoreFunctions, TAllocator> factory)
            where TAllocator : IAllocator<ScanStoreFunctions>
        {
            using var log = new NullDevice();
            using var store = new TsavoriteKV<ScanStoreFunctions, TAllocator>(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                PageSize = MinKvLogPageSize,
                LogMemorySize = 1L << 20,
                MutableFraction = 1
            }, StoreFunctions.Create(new ScanKeyComparer(distinct: false)), factory);
            using var session = store.NewSession<TestSpanByteKey, PinnedSpanByte, SpanByteAndMemory, Empty, CopyOnlyFunctions>(new());
            var key = BitConverter.GetBytes(1L);
            var discardedAddress = store.Log.TailAddress;
            _ = session.BasicContext.Upsert(TestSpanByteKey.FromArray(key), BitConverter.GetBytes(10L));

            // Model a record that recovery discards: recovery clears Sealed on each page it loads into memory, then invalidates the
            // records of the version it discards.
            store.hlogBase._wrapper.CreateLogRecord(discardedAddress).InfoRef.SetInvalid();

            // Past the bound, the upsert inserts a new record of key 1, leaving the discarded record invalid and not sealed.
            var bound = store.Log.TailAddress;
            _ = session.BasicContext.Upsert(TestSpanByteKey.FromArray(key), BitConverter.GetBytes(11L));
            var discarded = store.hlogBase._wrapper.CreateLogRecord(discardedAddress).Info;
            Assert.That(discarded.Invalid && !discarded.IsSealed, Is.True);

            // The scan must skip the discarded record although key 1 now has a record past the bound.
            long cursor = 0;
            var scanner = new CollectingScan();
            _ = session.ScanCursor(ref cursor, long.MaxValue, scanner, bound, maxAddress: bound);
            Assert.That(scanner.records, Is.Empty, "An invalid unsealed record must not be reported, even if its key has a record past the bound.");
        }

        /// <summary>
        /// A streaming checkpoint must not stream a key deleted in its version, even when the key is re-created during phase 2. The re-created
        /// record lies past phase 2's bound, the tail it sampled, so it belongs to the next version and is not streamed. Other bounded lookup
        /// scans report an elided record whose key has a record past their bound, because such a key existed at the bound. Phase 2 must not:
        /// here the elided record holds the key's value from before the delete, so reporting it would stream a deleted key.
        /// </summary>
        /// <param name="objectAllocator">Use the object allocator rather than the span-byte allocator.</param>
        [Test]
        public async Task StreamingSkipsElidedRecordOfKeyRecreatedAfterSample([Values] bool objectAllocator)
        {
            if (objectAllocator)
                await CheckRecreatedKey<ObjectAllocator<ScanStoreFunctions>>((settings, functions) => new(settings, functions));
            else
                await CheckRecreatedKey<SpanByteAllocator<ScanStoreFunctions>>((settings, functions) => new(settings, functions));
        }

        /// <summary>Runs <see cref="StreamingSkipsElidedRecordOfKeyRecreatedAfterSample"/> for one allocator.</summary>
        async Task CheckRecreatedKey<TAllocator>(Func<AllocatorSettings, ScanStoreFunctions, TAllocator> factory)
            where TAllocator : IAllocator<ScanStoreFunctions>
        {
            // Each key has its own tag chain (distinct: true), so key 1's chain holds only key 1's records.
            using var log = new NullDevice();
            using var store = new TsavoriteKV<ScanStoreFunctions, TAllocator>(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                PageSize = MinKvLogPageSize,
                LogMemorySize = 1L << 20,
                MutableFraction = 1,
                CheckpointDir = MethodTestDir
            }, StoreFunctions.Create(new ScanKeyComparer(distinct: true)), factory);
            using var session = store.NewSession<TestSpanByteKey, PinnedSpanByte, SpanByteAndMemory, Empty, CopyOnlyFunctions>(new());
            var key = BitConverter.GetBytes(1L);

            // Key 0 is written first, so phase 2 streams it first, and its callback below runs before phase 2 reaches key 1's records.
            _ = session.BasicContext.Upsert(TestSpanByteKey.FromArray(BitConverter.GetBytes(0L)), BitConverter.GetBytes(100L));

            // Key 1 is written as 5, then 6, then deleted, all before the checkpoint:
            // - writing 6 copies the 5-record, its chain's only record, and so elides it: it is sealed and invalid but not a tombstone,
            //   and still holds 5;
            // - the delete makes the 6-record a tombstone and elides it too, removing key 1's hash-index entry.
            var preDeleteAddress = store.Log.TailAddress;
            _ = session.BasicContext.Upsert(TestSpanByteKey.FromArray(key), BitConverter.GetBytes(5L));
            _ = session.BasicContext.Upsert(TestSpanByteKey.FromArray(key), BitConverter.GetBytes(6L));
            _ = session.BasicContext.Delete(TestSpanByteKey.FromArray(key));
            var preDelete = store.hlogBase._wrapper.CreateLogRecord(preDeleteAddress).Info;
            Assert.That(preDelete.Invalid && preDelete.IsSealed && !preDelete.Tombstone, Is.True, "The update must elide the first record.");

            // Key 0's callback re-creates key 1 as 1, past phase 2's bound. When phase 2 then reaches the 5-record, key 1 has a record past
            // the bound, so a scan that reported elided records of such keys would stream key 1 as 5. Phase 2 skips every invalid record.
            var stream = new CollectingStream(() =>
            {
                Assert.That(store.stateMachineDriver.SystemState.Phase, Is.EqualTo(Phase.IN_PROGRESS), "The key must be re-created during phase 2.");
                _ = session.BasicContext.Upsert(TestSpanByteKey.FromArray(key), BitConverter.GetBytes(1L));
            });
            _ = await store.TakeFullCheckpointAsync(CheckpointType.StreamingSnapshot, streamingSnapshotIteratorFunctions: stream)
                .AsTask().WaitAsync(TimeSpan.FromSeconds(15));

            // In the checkpoint's version only key 0 exists.
            Assert.That(stream.records, Is.EquivalentTo(new Dictionary<long, long> { [0] = 100 }),
                "A key deleted before the checkpoint must not be streamed with its pre-delete value.");
        }
    }
}