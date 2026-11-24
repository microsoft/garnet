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
    using ClosedScanStoreFunctions = StoreFunctions<SpanByteComparer, DefaultRecordTriggers>;

    /// <summary>
    /// Tests that a pull scan that includes closed records, which are sealed or invalid, never returns an invalid record read from disk whose
    /// key or value is held out of line.
    /// </summary>
    /// <remarks>
    /// The object allocator keeps a long key in an overflow allocation in its page's ObjectIdMap, and the record holds the allocation's id.
    /// A flush writes no objects for an invalid record, so on disk that field still holds an id into the map of the page that was flushed,
    /// which no longer exists. Loading a page into a scan frame deserializes objects only for valid records, into the scan's own map, so such
    /// an id names whatever this scan put in that slot, if anything.
    /// </remarks>
    [TestFixture]
    internal class ClosedRecordScanTests : TestBase
    {
        [SetUp]
        public void Setup() => DeleteDirectory(MethodTestDir, wait: true);

        [TearDown]
        public void TearDown() => OnTearDown();

        /// <summary>
        /// A scan that includes closed records reads an evicted page that holds an elided record with an overflow key, then a live record with
        /// an overflow key. It must return only the live record, intact. Returning the elided record would read its stale key id, which can
        /// name the live record's key in the scan's map; moving past it would then dispose that id, clearing the live record's key before the
        /// scan reaches it.
        /// </summary>
        [Test]
        public void ScanSkipsEvictedInvalidRecordWithOverflowKey()
        {
            using var log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "scan.log"), deleteOnClose: true);
            using var objectLog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "scan.obj.log"), deleteOnClose: true);
            using var store = new TsavoriteKV<ClosedScanStoreFunctions, ObjectAllocator<ClosedScanStoreFunctions>>(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                ObjectLogDevice = objectLog,
                PageSize = MinKvLogPageSize,
                LogMemorySize = 1L << 20,
                MutableFraction = 1
            }, StoreFunctions.Create(SpanByteComparer.Instance), (settings, functions) => new(settings, functions));
            using var session = store.NewSession<TestSpanByteKey, PinnedSpanByte, SpanByteAndMemory, Empty, SpanByteFunctions<Empty>>(new());
            var basic = session.BasicContext;

            // Keys of 2048 bytes, which the object allocator keeps in overflow allocations; their first eight bytes tell them apart.
            var deletedKey = new byte[2048];
            BitConverter.TryWriteBytes(deletedKey, 1L);
            var liveKey = new byte[2048];
            BitConverter.TryWriteBytes(liveKey, 2L);

            // Deleting key 1's only record in place makes it a tombstone and elides it: the record becomes invalid but keeps its overflow
            // key. Key 2's record follows it on the same page.
            var deletedAddress = store.Log.TailAddress;
            _ = basic.Upsert(TestSpanByteKey.FromArray(deletedKey), BitConverter.GetBytes(10L));
            _ = basic.Delete(TestSpanByteKey.FromArray(deletedKey));
            var liveAddress = store.Log.TailAddress;
            _ = basic.Upsert(TestSpanByteKey.FromArray(liveKey), BitConverter.GetBytes(20L));

            var deletedRecord = store.hlogBase._wrapper.CreateLogRecord(deletedAddress);
            Assert.That(deletedRecord.Info.Invalid && deletedRecord.DataHeader.KeyIsOverflow, Is.True,
                "Key 1's record must be elided and keep its overflow key.");
            Assert.That(store.hlogBase._wrapper.CreateLogRecord(liveAddress).DataHeader.KeyIsOverflow, Is.True, "Key 2 must be an overflow key.");

            // Write both records to disk and evict them, so that the scan reads them through a frame.
            store.Log.FlushAndEvict(wait: true);
            Assert.That(store.Log.HeadAddress, Is.GreaterThan(liveAddress), "Both records must be evicted.");

            // The scan must return key 2 alone, with its value. A tombstone has no value, so one would be recorded as -1.
            var returned = new List<(long address, long key, long value)>();
            using (var iter = store.Log.Scan(store.Log.BeginAddress, store.Log.TailAddress, includeClosedRecords: true))
            {
                while (iter.GetNext())
                    returned.Add((iter.CurrentAddress, BitConverter.ToInt64(iter.Key), iter.Info.Tombstone ? -1 : BitConverter.ToInt64(iter.ValueSpan)));
            }
            (long address, long key, long value)[] expected = [(liveAddress, 2L, 20L)];
            Assert.That(returned, Is.EqualTo(expected));
        }
    }
}