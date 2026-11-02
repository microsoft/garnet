// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;
using FillFunctions = Tsavorite.test.recovery.SnapshotReferenceTests.FillFunctions;
using FillInput = Tsavorite.test.recovery.SnapshotReferenceTests.FillInput;
using FillOutput = Tsavorite.test.recovery.SnapshotReferenceTests.FillOutput;
using SharedListFunctions = Tsavorite.test.recovery.CheckpointSharedObjectMutationTests.SharedListFunctions;
using SharedListHeapObject = Tsavorite.test.recovery.CheckpointSharedObjectMutationTests.SharedListHeapObject;
using SharedListSerializer = Tsavorite.test.recovery.CheckpointSharedObjectMutationTests.SharedListSerializer;

namespace Tsavorite.test.recovery
{
    using ClassAllocator = ObjectAllocator<StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>>;
    using ClassStoreFunctions = StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>;

    /// <summary>
    /// A scan locks a record's key while it copies the record unless the record's page lies entirely below SafeReadOnlyAddress. At or above
    /// SafeReadOnlyAddress an in-place update can change the record, and a page that holds SafeReadOnlyAddress shares its ObjectIdMap with mutable
    /// records, which can reuse a released slot. On a page entirely below it, the only change is the release of a superseded record's out-of-line
    /// fields, so the scan copies without a lock, and a field released while the record was copied resolves as nothing.
    /// </summary>
    [TestFixture]
    internal unsafe class ImmutableRegionScanTests : TestBase
    {
        const int OverflowValueLength = 1000;
        static readonly TimeSpan Unblocked = TimeSpan.FromSeconds(10);
        static readonly TimeSpan Blocked = TimeSpan.FromMilliseconds(300);

        /// <summary>The lowest key of the records <see cref="MakeImmutable"/> pads a page with; scans do not report them.</summary>
        const int FillerKeyBase = 1 << 20;

        /// <summary>A record a scan reported: its address, key, whether it was invalid, and a description of its value.</summary>
        internal readonly record struct Reported(long Address, int Key, bool Invalid, string Value);

        /// <summary>The out-of-line field a simulated release has freed (<c>Slot</c>), or has also set to <see cref="ObjectIdMap.InvalidObjectId"/>
        /// (<c>Id</c>), before it publishes the record's inline header.</summary>
        public enum ReleasedField
        {
            ObjectValueSlot,
            ObjectValueId,
            OverflowValueSlot,
            OverflowValueId,
            OverflowKeySlot,
            OverflowKeyId,
        }

        string dir;
        IDevice log, objlog;
        TsavoriteKV<ClassStoreFunctions, ClassAllocator> store;
        int nextFillerKey;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            dir = MethodTestDir;
            _ = Directory.CreateDirectory(dir);
            log = Devices.CreateLogDevice(Path.Join(dir, "Scan.log"), deleteOnClose: false);
            objlog = Devices.CreateLogDevice(Path.Join(dir, "Scan.obj.log"), deleteOnClose: false);
            nextFillerKey = FillerKeyBase;
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

        void CreateStore(bool overflowKeys, long logMemorySize = 1L << 16)
            => store = new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                ObjectLogDevice = objlog,
                MutableFraction = 0.9,
                LogMemorySize = logMemorySize,
                PageSize = 1L << 13,
                MaxInlineKeySize = overflowKeys ? 0 : KVSettings.DefaultMaxInlineKeySize,
                MaxInlineValueSize = 0,
                CheckpointDir = dir
            }, StoreFunctions.Create(new TestObjectKey.Comparer(), () => new SharedListSerializer())
             , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));

        /// <summary>
        /// An exclusive lock on the key of a record on a page entirely below SafeReadOnlyAddress does not hold up a scan. One on the key of a record below
        /// SafeReadOnlyAddress on the page that holds it, or at or above SafeReadOnlyAddress, does until it is released.
        /// </summary>
        [Test]
        [Category("TsavoriteKV")]
        public void ScanLocksAKeyUnlessItsPageIsBelowSafeReadOnlyAddress()
        {
            CreateStore(overflowKeys: false);
            const int immutableKey = 1;
            var immutableAddress = UpsertList(immutableKey);
            var boundaryPageFiller = MakeImmutable(avoidKeys: [immutableKey]);

            // Each key is in its own bucket, and none shares the bucket of the filler that starts the boundary page, which the scan also locks.
            var boundaryKey = KeyInAnotherBucket(immutableKey, immutableKey, boundaryPageFiller);
            var boundaryAddress = UpsertList(boundaryKey);
            _ = MakeImmutable(wholePage: false);
            var mutableKey = KeyInAnotherBucket(boundaryKey, immutableKey, boundaryPageFiller, boundaryKey);
            var mutableAddress = UpsertList(mutableKey);

            var safeReadOnlyAddress = store.hlogBase.SafeReadOnlyAddress;
            Assert.That(EndOfPage(immutableAddress), Is.LessThanOrEqualTo(safeReadOnlyAddress));
            Assert.That(boundaryAddress, Is.LessThan(safeReadOnlyAddress));
            Assert.That(EndOfPage(boundaryAddress), Is.GreaterThan(safeReadOnlyAddress));
            Assert.That(mutableAddress, Is.GreaterThanOrEqualTo(safeReadOnlyAddress));
            int[] keys = [immutableKey, boundaryKey, mutableKey];

            var hei = LockExclusive(immutableKey);
            ReportingScan scan = new();
            Task scanTask;
            try
            {
                scanTask = Task.Run(() => PushScan(scan, bounded: false));
                Assert.That(scanTask.Wait(Unblocked), Is.True, "The scan waited for the lock on the key of a record on a page below SafeReadOnlyAddress");
            }
            finally
            {
                store.LockTable.UnlockExclusive(ref hei);
            }
            Assert.That(scan.Records.Select(r => r.Key), Is.EqualTo(keys));

            foreach (var (lockedKey, where) in new[] { (boundaryKey, "below SafeReadOnlyAddress on the page that holds it"), (mutableKey, "at or above SafeReadOnlyAddress") })
            {
                hei = LockExclusive(lockedKey);
                scan = new();
                try
                {
                    scanTask = Task.Run(() => PushScan(scan, bounded: false));

                    // The scan reports the record on the page below SafeReadOnlyAddress, then reaches the locked one.
                    Assert.That(scan.FirstReported.Wait(Unblocked), Is.True, "The scan did not report the record on the page below SafeReadOnlyAddress");
                    Assert.That(scanTask.Wait(Blocked), Is.False, $"The scan copied a record {where} without locking its key");
                }
                finally
                {
                    store.LockTable.UnlockExclusive(ref hei);
                }
                Assert.That(scanTask.Wait(Unblocked), Is.True, "The scan did not finish after the lock was released");
                Assert.That(scan.Records.Select(r => r.Key), Is.EqualTo(keys));
            }
        }

        /// <summary>
        /// At or above SafeReadOnlyAddress, a scan that includes closed records locks the key of a Sealed and Invalid record, which may be a new record
        /// whose operation has not yet CAS'd it or one an elision is disposing. It does not lock an Invalid record that is not Sealed, which only recovery
        /// produces and whose key may be empty, and it does not report one.
        /// </summary>
        [Test]
        [Category("TsavoriteKV")]
        public void BoundedScanLocksTheKeyOfASealedInvalidRecord([Values] bool isSealed)
        {
            CreateStore(overflowKeys: false);
            const int key = 1;
            var address = UpsertList(key);
            Assert.That(address, Is.GreaterThanOrEqualTo(store.hlogBase.SafeReadOnlyAddress));

            var record = store.hlogBase._wrapper.CreateLogRecord(address);
            if (isSealed)
                record.InfoRef.SealAndInvalidate();
            else
                record.InfoRef.SetInvalid();

            var hei = LockExclusive(key);
            ReportingScan scan = new();
            Task scanTask;
            try
            {
                // A bounded scan includes closed records.
                scanTask = Task.Run(() => PushScan(scan, bounded: true));
                Assert.That(scan.Started.Wait(Unblocked), Is.True, "The scan did not start");
                if (isSealed)
                    Assert.That(scanTask.Wait(Blocked), Is.False, "The scan copied a Sealed and Invalid record without locking its key");
                else
                    Assert.That(scanTask.Wait(Unblocked), Is.True, "The scan waited for the lock on the key of an Invalid record that is not Sealed");
            }
            finally
            {
                store.LockTable.UnlockExclusive(ref hei);
            }
            Assert.That(scanTask.Wait(Unblocked), Is.True, "The scan did not finish after the lock was released");
            if (!isSealed)
                Assert.That(scan.Records, Is.Empty, "The scan reported an Invalid record that is not Sealed");
            record.InfoRef.UnsealAndValidate();
        }

        /// <summary>
        /// On a page entirely below SafeReadOnlyAddress a scan copies a record without a lock, so the copy can be taken while the operation that superseded
        /// the record releases a field: after the release frees the field's slot, and possibly after it sets the field to InvalidObjectId, but before it
        /// publishes the inline header. A released value is reported as nothing, and a record whose key was released is not reported.
        /// </summary>
        [Test]
        [Category("TsavoriteKV")]
        public void ScanOfARecordWhoseFieldIsBeingReleasedReportsNothing([Values] ReleasedField field, [Values] bool pushScan)
        {
            var keyReleased = field is ReleasedField.OverflowKeySlot or ReleasedField.OverflowKeyId;
            var overflowValueReleased = field is ReleasedField.OverflowValueSlot or ReleasedField.OverflowValueId;
            CreateStore(overflowKeys: keyReleased);
            var objectAddress = UpsertList(1);
            var overflowAddress = UpsertOverflow(2);
            var controlAddress = UpsertList(3);
            _ = MakeImmutable();

            var target = overflowValueReleased ? overflowAddress : objectAddress;
            var record = store.hlogBase._wrapper.CreateLogRecord(target);
            var (_, fieldAddress) = keyReleased
                ? record.DataHeader.GetKeyFieldInfo(record.physicalAddress)
                : record.DataHeader.GetValueFieldInfo(record.physicalAddress);
            var slotWord = *(int*)fieldAddress;
            var map = ((ObjectAllocatorImpl<ClassStoreFunctions>)store.hlogBase).GetObjectIdMap(target);
            var index = ObjectIdMap.GetIndex(slotWord);
            var component = map.objectArray.Get(index);

            if (field is ReleasedField.ObjectValueSlot or ReleasedField.OverflowValueSlot or ReleasedField.OverflowKeySlot)
                map.objectArray.Set(index, null);
            else
            {
                map.objectArray.Set(index, null);
                *(int*)fieldAddress = ObjectIdMap.InvalidObjectId;
            }

            List<Reported> records;
            try
            {
                if (pushScan)
                {
                    ReportingScan scan = new();
                    PushScan(scan, bounded: false);
                    records = scan.Records;
                }
                else
                    records = PullScan();
            }
            finally
            {
                *(int*)fieldAddress = slotWord;
                map.objectArray.Set(index, component);
            }

            List<Reported> expected = [];
            if (!keyReleased)
                expected.Add(new(objectAddress, 1, Invalid: false, overflowValueReleased ? "list:1" : "null"));
            expected.Add(new(overflowAddress, 2, Invalid: false, overflowValueReleased ? "overflow:0" : $"overflow:{OverflowValueLength}"));
            expected.Add(new(controlAddress, 3, Invalid: false, "list:1"));
            Assert.That(records, Is.EqualTo(expected));
        }

        /// <summary>
        /// A copy taken without a lock is retaken if the record was closed or its header was published while it was copied: an elision closes the record
        /// before it releases the key and value, and each release publishes a new header. Other changes to a record's info, such as its snapshot marker,
        /// do not affect what the copy reports, so they do not cause a retry.
        /// </summary>
        [Test]
        [Category("TsavoriteKV")]
        public void CopyCheckDetectsAClosingOrAHeaderPublish()
        {
            CreateStore(overflowKeys: false);
            var address = UpsertList(1);
            _ = MakeImmutable();

            var record = store.hlogBase._wrapper.CreateLogRecord(address);
            var size = (int)record.AllocatedSize;
            var copy = GC.AllocateArray<byte>(size, pinned: true);
            var copyAddress = (long)Unsafe.AsPointer(ref copy[0]);
            Buffer.MemoryCopy((void*)record.physicalAddress, (void*)copyAddress, size, size);
            bool Changed() => ScanIteratorBase<ClassAllocator>.ChangedWhileCopied(record.physicalAddress, copyAddress);

            Assert.That(Changed(), Is.False, "An unchanged record was reported changed");

            record.InfoRef.HasSnapshotObjectLogReference = true;
            Assert.That(Changed(), Is.False, "A marker change was reported as a change to what the copy holds");
            record.InfoRef.HasSnapshotObjectLogReference = false;

            record.InfoRef.SealAndInvalidate();
            Assert.That(Changed(), Is.True, "A record closed while it was copied was not detected");
            record.InfoRef.UnsealAndValidate();
            Assert.That(Changed(), Is.False);

            var header = record.DataHeader;
            var published = header;
            published.RecordType = (byte)(header.RecordType + 1);
            record.SetDataHeader(published);
            Assert.That(Changed(), Is.True, "A header published while the record was copied was not detected");
            record.SetDataHeader(header);
            Assert.That(Changed(), Is.False);
            GC.KeepAlive(copy);
        }

        /// <summary>
        /// Scans run while CopyUpdates supersede records on pages entirely below SafeReadOnlyAddress, releasing each source's value and eliding
        /// single-version sources, which also releases an overflow key. Every record a scan reports holds a valid value or nothing.
        /// </summary>
        [Test]
        [Category("TsavoriteKV")]
        public void ScansDuringCopyUpdatesOfImmutableRecordsReportValidValuesOrNothing([Values] bool overflowKeys)
        {
            CreateStore(overflowKeys, logMemorySize: 1L << 22);
            const int keyCount = 64;
            const int maxRounds = 200;
            for (var key = 0; key < keyCount; ++key)
                _ = UpsertList(key);

            using CancellationTokenSource stop = new();
            Exception scanFailure = null;
            var scans = 0;
            var scanner = Task.Run(() =>
            {
                try
                {
                    while (!stop.IsCancellationRequested)
                    {
                        foreach (var record in PullScan())
                            ValidateScannedRecord(record, keyCount);

                        // A lookup's traceback reads the keys of records in the chain, which an elision may be releasing, so only inline keys are looked up.
                        if (!overflowKeys)
                        {
                            ReportingScan scan = new();
                            PushScan(scan, bounded: true);
                            foreach (var record in scan.Records)
                                ValidateScannedRecord(record, keyCount);
                        }
                        ++scans;
                    }
                }
                catch (Exception ex)
                {
                    scanFailure = ex;
                }
            });

            using (var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, SharedListFunctions>(new SharedListFunctions()))
            {
                var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(1);
                for (var round = 0; round < maxRounds && DateTime.UtcNow < deadline && Volatile.Read(ref scanFailure) is null; ++round)
                {
                    _ = MakeImmutable();
                    for (var key = 0; key < keyCount; ++key)
                    {
                        TestObjectInput input = new() { value = round };
                        TestObjectOutput output = new();
                        var status = session.BasicContext.RMW(new TestObjectKey { key = key }, ref input, ref output);
                        if (status.IsPending)
                            _ = session.BasicContext.CompletePending(wait: true);
                    }
                }
            }

            stop.Cancel();
            Assert.That(scanner.Wait(Unblocked), Is.True, "The scanner did not stop");
            Assert.That(scanFailure, Is.Null, $"A scan failed: {scanFailure}");
            Assert.That(scans, Is.GreaterThan(0), "No scan completed");
        }

        /// <summary>A valid record has its own key and a list value, or nothing if its value was released while it was copied. An invalid record was
        /// superseded and released; what a scan reports for it is not checked here.</summary>
        static void ValidateScannedRecord(Reported record, int keyCount)
        {
            if (record.Invalid)
                return;
            if (record.Key < 0 || record.Key >= keyCount)
                throw new AssertionException($"A scan reported key {record.Key} at {record.Address}");
            if (!record.Value.StartsWith("list:", StringComparison.Ordinal) && record.Value != "null" && record.Value != $"inline:{ObjectIdMap.ObjectIdSize}")
                throw new AssertionException($"A scan reported value {record.Value} for key {record.Key} at {record.Address}");
        }

        long UpsertList(int key)
        {
            using var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, SharedListFunctions>(new SharedListFunctions());
            var address = store.Log.TailAddress;
            _ = session.BasicContext.Upsert(new TestObjectKey { key = key }, new SharedListHeapObject([key]), Empty.Default);
            return address;
        }

        long UpsertOverflow(int key)
        {
            using var session = store.NewSession<TestObjectKey, FillInput, FillOutput, Empty, FillFunctions>(new FillFunctions());
            var address = store.Log.TailAddress;
            var value = new byte[OverflowValueLength];
            value.AsSpan().Fill(1);
            _ = session.BasicContext.Upsert(new TestObjectKey { key = key }, value.AsSpan(), Empty.Default);
            return address;
        }

        /// <summary>
        /// Make the records so far immutable: shift ReadOnlyAddress to the tail and wait for SafeReadOnlyAddress to reach it.
        /// </summary>
        /// <param name="wholePage">First fill the tail page with filler records, so the records so far lie on pages entirely below SafeReadOnlyAddress,
        /// where a scan copies them without a lock. The filler that does not fit starts the next page.</param>
        /// <param name="avoidKeys">Keys whose buckets no filler shares, so a test's lock on one of them holds up a scan only at that key's record.</param>
        /// <returns>The key of the last filler, which starts the next page unless the fillers filled the page exactly; -1 if there are no fillers.</returns>
        int MakeImmutable(bool wholePage = true, int[] avoidKeys = null)
        {
            var lastFiller = -1;
            if (wholePage)
            {
                var avoidBuckets = (avoidKeys ?? []).Select(BucketOf).ToHashSet();
                using var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, SharedListFunctions>(new SharedListFunctions());
                var page = store.hlogBase.GetPage(store.Log.TailAddress);
                while (store.hlogBase.GetPage(store.Log.TailAddress) == page)
                {
                    var key = nextFillerKey++;
                    if (avoidBuckets.Contains(BucketOf(key)))
                        continue;
                    _ = session.BasicContext.Upsert(new TestObjectKey { key = key }, new SharedListHeapObject([key]), Empty.Default);
                    lastFiller = key;
                }
            }
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
            Assert.That(store.hlogBase.SafeReadOnlyAddress, Is.EqualTo(store.Log.TailAddress), "The records are not below SafeReadOnlyAddress");
            return lastFiller;
        }

        long EndOfPage(long address) => store.hlogBase.GetLogicalAddressOfStartOfPage(store.hlogBase.GetPage(address) + 1);

        /// <summary>The first key after <paramref name="after"/> whose bucket is not that of any of <paramref name="others"/>; negative keys are ignored.</summary>
        int KeyInAnotherBucket(int after, params int[] others)
        {
            var buckets = others.Where(k => k >= 0).Select(BucketOf).ToHashSet();
            var key = after + 1;
            while (buckets.Contains(BucketOf(key)))
                ++key;
            return key;
        }

        long BucketOf(int key)
        {
            HashEntryInfo hei = new(store.storeFunctions.GetKeyHashCode64(new TestObjectKey { key = key }));
            store.FindOrCreateTag(ref hei, store.Log.BeginAddress);
            return (long)hei.bucket;
        }

        HashEntryInfo LockExclusive(int key)
        {
            HashEntryInfo hei = new(store.storeFunctions.GetKeyHashCode64(new TestObjectKey { key = key }));
            store.FindOrCreateTag(ref hei, store.Log.BeginAddress);
            Assert.That(store.LockTable.TryLockExclusive(ref hei), Is.True, $"Could not lock the bucket of key {key}");
            return hei;
        }

        List<Reported> PullScan()
        {
            List<Reported> records = [];
            using var iter = store.Log.Scan(store.Log.BeginAddress, store.Log.TailAddress);
            while (iter.GetNext())
            {
                var record = Describe(iter, iter.CurrentAddress);
                if (record.Key < FillerKeyBase)
                    records.Add(record);
            }
            return records;
        }

        void PushScan(ReportingScan scan, bool bounded)
        {
            using var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, SharedListFunctions>(new SharedListFunctions());
            if (bounded)
                _ = session.IterateLookupSnapshot(ref scan);
            else
            {
                long cursor = 0;
                _ = session.ScanCursor(ref cursor, long.MaxValue, scan, endAddress: store.Log.TailAddress);
            }
            if (scan.Failure is not null)
                throw new AssertionException($"The scan failed: {scan.Failure}");
        }

        static Reported Describe<TSourceLogRecord>(in TSourceLogRecord logRecord, long address)
            where TSourceLogRecord : ISourceLogRecord
        {
            var value = logRecord.DataHeader.ValueIsObject ? (logRecord.ValueObject is SharedListHeapObject list ? $"list:{list.Items.Count}" : "null")
                : logRecord.DataHeader.ValueIsOverflow ? $"overflow:{logRecord.ValueSpan.Length}"
                : $"inline:{logRecord.ValueSpan.Length}";
            return new(address, logRecord.Key.AsRef<TestObjectKey>().key, logRecord.Info.Invalid, value);
        }

        /// <summary>Records what a push scan reports, except fillers, and signals its start and its first report so a test can tell where it is.</summary>
        internal sealed class ReportingScan : IScanIteratorFunctions
        {
            internal readonly List<Reported> Records = [];
            internal readonly ManualResetEventSlim Started = new();
            internal readonly ManualResetEventSlim FirstReported = new();
            internal Exception Failure;

            public bool Reader<TSourceLogRecord>(in TSourceLogRecord logRecord, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                where TSourceLogRecord : ISourceLogRecord
            {
                cursorRecordResult = CursorRecordResult.Accept;
                var record = Describe(in logRecord, recordMetadata.Address);
                if (record.Key >= FillerKeyBase)
                    return true;
                Records.Add(record);
                FirstReported.Set();
                return true;
            }

            public void OnException(Exception exception, long numberOfRecords) => Failure = exception;

            public bool OnStart(long beginAddress, long endAddress)
            {
                Started.Set();
                return true;
            }

            public void OnStop(bool completed, long numberOfRecords) { }
        }
    }
}