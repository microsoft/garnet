// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Concurrent;
using System.IO;
using System.Threading;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

#pragma warning disable IDE0060 // Unused parameter

namespace Tsavorite.test
{
    using ExtAllocator = ObjectAllocator<StoreFunctions<TestObjectKey.Comparer, RecordTriggersExtTests.ExtRecordTriggers>>;
    using ExtStoreFunctions = StoreFunctions<TestObjectKey.Comparer, RecordTriggersExtTests.ExtRecordTriggers>;

    /// <summary>
    /// Tests for the trigger extensions added for BfTree (RangeIndex) lifecycle integration:
    /// <list type="bullet">
    /// <item><see cref="IRecordTriggers.OnFlush(ref LogRecord, long)"/> receives the correct logical address.</item>
    /// <item><see cref="IRecordTriggers.OnTruncate(long)"/> fires AFTER device truncation completes.</item>
    /// <item><see cref="IRecordTriggers.PostCopyToTail{TSrc}(in TSrc, long, ref LogRecord, long)"/>
    /// fires from <c>TryCopyToTail</c> with valid source/destination addresses.</item>
    /// </list>
    /// All tests use <see cref="ObjectAllocator{T}"/> because that is the allocator that fires
    /// <see cref="IRecordTriggers.OnFlush"/> (and is what Garnet's unified store uses for
    /// the RangeIndex stub records).
    /// </summary>
    [TestFixture]
    public class RecordTriggersExtTests : TestBase
    {
        /// <summary>Per-event log entries recorded by the test trigger.</summary>
        internal sealed class TriggerEvents
        {
            public readonly ConcurrentBag<long> FlushAddresses = new();
            public readonly ConcurrentBag<long> TruncateAddresses = new();
            public readonly ConcurrentBag<(long SrcAddr, long DstAddr)> PostCopyToTailEvents = new();

            public bool CallOnFlushFlag;
            public bool CallOnTruncateFlag;
            public bool CallPostCopyToTailFlag;
            public bool CallOnDiskReadFlag;

            public int FlushCount => FlushAddresses.Count;
            public int TruncateCount => TruncateAddresses.Count;
            public int PostCopyCount => PostCopyToTailEvents.Count;
        }

        internal struct ExtRecordTriggers : IRecordTriggers
        {
            internal readonly TriggerEvents events;
            public ExtRecordTriggers(TriggerEvents events) { this.events = events; }

            public readonly bool CallOnFlush => events?.CallOnFlushFlag ?? false;
            public readonly bool CallOnEvict => false;
            public readonly bool CallOnDiskRead => events?.CallOnDiskReadFlag ?? false;
            public readonly bool CallPostCopyToTail => events?.CallPostCopyToTailFlag ?? false;
            public readonly bool CallOnTruncate => events?.CallOnTruncateFlag ?? false;

            public readonly void OnFlush(ref LogRecord logRecord, long logicalAddress)
            {
                events?.FlushAddresses.Add(logicalAddress);
            }

            public readonly void OnTruncate(long newBeginAddress)
            {
                events?.TruncateAddresses.Add(newBeginAddress);
            }

            public readonly void PostCopyToTail<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, long srcLogicalAddress,
                                                                  ref LogRecord dstLogRecord, long dstLogicalAddress)
                where TSourceLogRecord : ISourceLogRecord
#if NET9_0_OR_GREATER
                    , allows ref struct
#endif
            {
                events?.PostCopyToTailEvents.Add((srcLogicalAddress, dstLogicalAddress));
            }
        }

        private TsavoriteKV<ExtStoreFunctions, ExtAllocator> store;
        private IDevice log, objlog;
        private TriggerEvents events;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "RecordTriggersExtTests.log"), deleteOnClose: true);
            objlog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "RecordTriggersExtTests.obj.log"), deleteOnClose: true);
            events = new TriggerEvents();
            store = new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                ObjectLogDevice = objlog,
                MutableFraction = 0.1,
                LogMemorySize = 1L << 15,
                PageSize = 1L << 10,
            }, StoreFunctions.Create(new TestObjectKey.Comparer(), () => new TestObjectValue.Serializer(),
                    new ExtRecordTriggers(events))
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));
        }

        [TearDown]
        public void TearDown()
        {
            store?.Dispose(); store = null;
            log?.Dispose(); log = null;
            objlog?.Dispose(); objlog = null;
            OnTearDown();
        }

        private void InsertN(int n)
        {
            using var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, int, TestObjectFunctionsDelete>(new TestObjectFunctionsDelete());
            var bContext = session.BasicContext;
            for (int i = 0; i < n; i++)
                _ = bContext.Upsert(new TestObjectKey { key = i }, new TestObjectValue { value = i }, 0);
        }

        // -- OnFlush(addr) tests --

        /// <summary>
        /// Verifies that <see cref="IRecordTriggers.OnFlush(ref LogRecord, long)"/> fires on
        /// <c>FlushAndEvict</c> with logical addresses that fall in [BeginAddress, TailAddress).
        /// </summary>
        [Test, Category("TsavoriteKV"), Category("RecordTriggers")]
        public void OnFlushReceivesCorrectLogicalAddress()
        {
            events.CallOnFlushFlag = true;
            InsertN(200);

            store.Log.FlushAndEvict(wait: true);

            ClassicAssert.Greater(events.FlushCount, 0,
                $"OnFlush should fire at least once after FlushAndEvict (got {events.FlushCount})");

            var tail = store.Log.TailAddress;
            var ba = store.Log.BeginAddress;
            foreach (var addr in events.FlushAddresses)
            {
                ClassicAssert.GreaterOrEqual(addr, ba, $"flush addr {addr} below BeginAddress {ba}");
                ClassicAssert.Less(addr, tail, $"flush addr {addr} >= TailAddress {tail}");
            }
        }

        /// <summary>
        /// Verifies that <see cref="IStoreFunctions.CallOnFlush"/> false skips the OnFlush callback.
        /// </summary>
        [Test, Category("TsavoriteKV"), Category("RecordTriggers")]
        public void CallOnFlushFalseSkipsCallback()
        {
            events.CallOnFlushFlag = false;
            InsertN(200);
            store.Log.FlushAndEvict(wait: true);

            ClassicAssert.AreEqual(0, events.FlushCount, "OnFlush should not fire when CallOnFlush=false");
        }

        // -- OnTruncate tests --

        /// <summary>
        /// Verifies that <see cref="IRecordTriggers.OnTruncate(long)"/> fires from
        /// <c>ShiftBeginAddress(_, truncateLog: true)</c> AFTER the device truncation completes,
        /// receiving the new BeginAddress.
        /// </summary>
        [Test, Category("TsavoriteKV"), Category("RecordTriggers")]
        public void OnTruncateFiresWithNewBeginAddress()
        {
            events.CallOnTruncateFlag = true;
            InsertN(200);
            store.Log.FlushAndEvict(wait: true);

            var newBA = store.Log.HeadAddress;
            store.Log.ShiftBeginAddress(newBA, truncateLog: true);

            // TruncateUntilAddress runs on a Task.Run; wait up to 5s for fire.
            for (int wait = 0; wait < 100 && events.TruncateCount == 0; wait++)
                Thread.Sleep(50);

            ClassicAssert.GreaterOrEqual(events.TruncateCount, 1, "OnTruncate should fire at least once");
            ClassicAssert.Contains(newBA, events.TruncateAddresses,
                $"OnTruncate should receive the new BeginAddress {newBA}");
        }

        /// <summary>
        /// Verifies that <see cref="IStoreFunctions.CallOnTruncate"/> false skips the callback.
        /// </summary>
        [Test, Category("TsavoriteKV"), Category("RecordTriggers")]
        public void CallOnTruncateFalseSkipsCallback()
        {
            events.CallOnTruncateFlag = false;
            InsertN(200);
            store.Log.FlushAndEvict(wait: true);
            store.Log.ShiftBeginAddress(store.Log.HeadAddress, truncateLog: true);
            Thread.Sleep(500);

            ClassicAssert.AreEqual(0, events.TruncateCount, "OnTruncate should not fire when CallOnTruncate=false");
        }

        // -- PostCopyToTail tests --

        /// <summary>
        /// Verifies that <see cref="IRecordTriggers.PostCopyToTail"/> fires from compaction with
        /// valid source and destination logical addresses (src in [oldBA, compactUntil],
        /// dst at/above pre-compact tail).
        /// </summary>
        [Test, Category("TsavoriteKV"), Category("RecordTriggers"), Category("Compaction")]
        public void PostCopyToTailFiresFromCompaction([Values] CompactionType compactionType)
        {
            events.CallPostCopyToTailFlag = true;
            events.CallOnDiskReadFlag = true;

            using var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, int, TestObjectFunctionsDelete>(new TestObjectFunctionsDelete());
            var bContext = session.BasicContext;

            const int N = 800;
            long compactUntil = 0;
            for (int i = 0; i < N; i++)
            {
                if (i == N / 2)
                    compactUntil = store.Log.TailAddress;
                _ = bContext.Upsert(new TestObjectKey { key = i }, new TestObjectValue { value = i }, 0);
            }

            // Snapshot pre-compact state.
            var oldBA = store.Log.BeginAddress;
            var preCompactTail = store.Log.TailAddress;
            store.Log.FlushAndEvict(wait: true);

            session.Compact(compactUntil, compactionType);

            ClassicAssert.Greater(events.PostCopyCount, 0,
                $"PostCopyToTail should fire at least once for compacted records (got {events.PostCopyCount})");

            // Every PostCopyToTail event should have:
            //  - srcAddr in [oldBA, compactUntil]: source was below compactUntil at compaction time.
            //  - dstAddr >= preCompactTail: record copied to the tail.
            foreach (var (srcAddr, dstAddr) in events.PostCopyToTailEvents)
            {
                ClassicAssert.GreaterOrEqual(srcAddr, oldBA,
                    $"src addr {srcAddr} below pre-compact BeginAddress {oldBA}");
                ClassicAssert.LessOrEqual(srcAddr, compactUntil,
                    $"src addr {srcAddr} above compactUntil {compactUntil}");
                ClassicAssert.GreaterOrEqual(dstAddr, preCompactTail,
                    $"dst addr {dstAddr} should be at/after pre-compact tail {preCompactTail}");
            }
        }

        /// <summary>
        /// Verifies that <see cref="IStoreFunctions.CallPostCopyToTail"/> false skips the callback.
        /// </summary>
        [Test, Category("TsavoriteKV"), Category("RecordTriggers"), Category("Compaction")]
        public void CallPostCopyToTailFalseSkipsCallback()
        {
            events.CallPostCopyToTailFlag = false;

            using var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, int, TestObjectFunctionsDelete>(new TestObjectFunctionsDelete());
            var bContext = session.BasicContext;
            for (int i = 0; i < 800; i++)
                _ = bContext.Upsert(new TestObjectKey { key = i }, new TestObjectValue { value = i }, 0);
            var compactUntil = store.Log.TailAddress;
            store.Log.FlushAndEvict(wait: true);
            session.Compact(compactUntil, CompactionType.Scan);

            ClassicAssert.AreEqual(0, events.PostCopyCount,
                "PostCopyToTail should not fire when CallPostCopyToTail=false");
        }

        // -- Default trigger flags --

        /// <summary>
        /// Default no-op trigger struct must have all the new flags returning false.
        /// </summary>
        [Test, Category("TsavoriteKV"), Category("RecordTriggers")]
        public void DefaultRecordTriggersHasAllNewFlagsFalse()
        {
            IRecordTriggers def = DefaultRecordTriggers.Instance;
            ClassicAssert.IsFalse(def.CallOnFlush);
            ClassicAssert.IsFalse(def.CallOnEvict);
            ClassicAssert.IsFalse(def.CallOnDiskRead);
            ClassicAssert.IsFalse(def.CallPostCopyToTail);
            ClassicAssert.IsFalse(def.CallOnTruncate);
        }

        /// <summary>
        /// SpanByteRecordTriggers (legacy no-op) must also have all flags false.
        /// </summary>
        [Test, Category("TsavoriteKV"), Category("RecordTriggers")]
        public void SpanByteRecordTriggersHasAllNewFlagsFalse()
        {
            IRecordTriggers def = SpanByteRecordTriggers.Instance;
            ClassicAssert.IsFalse(def.CallOnFlush);
            ClassicAssert.IsFalse(def.CallOnEvict);
            ClassicAssert.IsFalse(def.CallOnDiskRead);
            ClassicAssert.IsFalse(def.CallPostCopyToTail);
            ClassicAssert.IsFalse(def.CallOnTruncate);
        }
    }
}