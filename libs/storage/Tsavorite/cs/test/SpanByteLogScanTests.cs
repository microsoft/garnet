// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Text.Unicode;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.core.Utility;
using static Tsavorite.test.SpanByteIterationTests;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.spanbyte
{
    // Must be in a separate block so the "using SpanByteStoreFunctions" is the first line in its namespace declaration.
    struct SpanByteComparerModulo : IKeyComparer<SpanByte>
    {
        readonly long mod;

        internal SpanByteComparerModulo(long mod) => this.mod = mod;

        public bool Equals(ref SpanByte k1, ref SpanByte k2) => SpanByteComparer.StaticEquals(ref k1, ref k2);

        // Force collisions to create a chain
        public long GetHashCode64(ref SpanByte k)
        {
            long hash = SpanByteComparer.StaticGetHashCode64(ref k);
            return mod > 0 ? hash % mod : hash;
        }
    }
}

namespace Tsavorite.test.spanbyte
{
    using SpanByteStoreFunctions = StoreFunctions<SpanByte, SpanByte, SpanByteComparerModulo, SpanByteRecordDisposer>;

    [TestFixture]
    internal class SpanByteLogScanTests
    {
        private TsavoriteKV<SpanByte, SpanByte, SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>> store;
        private IDevice log;
        const int TotalRecords = 2000;
        const int PageSizeBits = 15;
        const int ComparerModulo = 100;

        [SetUp]
        public void Setup()
        {
            SpanByteComparerModulo comparer = new(0);
            foreach (var arg in TestContext.CurrentContext.Test.Arguments)
            {
                if (arg is HashModulo mod && mod == HashModulo.Hundred)
                {
                    comparer = new SpanByteComparerModulo(ComparerModulo);
                    continue;
                }
            }

            DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "test.log"), deleteOnClose: true);
            store = new(new()
            {
                IndexSize = 1L << 26,
                LogDevice = log,
                MemorySize = 1L << 25,
                PageSize = 1L << PageSizeBits
            }, StoreFunctions<SpanByte, SpanByte>.Create(comparer, SpanByteRecordDisposer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );
        }

        [TearDown]
        public void TearDown()
        {
            store?.Dispose();
            store = null;
            log?.Dispose();
            log = null;
            DeleteDirectory(MethodTestDir);
        }

        public class ScanFunctions : SpanByteFunctions<Empty>
        {
            // Right now this is unused but helped with debugging so I'm keeping it around.
            internal long insertedAddress;

            public override bool SingleWriter(ref SpanByte key, ref SpanByte input, ref SpanByte src, ref SpanByte dst, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo, WriteReason reason, ref RecordInfo recordInfo)
            {
                insertedAddress = upsertInfo.Address;
                return base.SingleWriter(ref key, ref input, ref src, ref dst, ref output, ref upsertInfo, reason, ref recordInfo);
            }
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void SpanByteScanCursorTest([Values(HashModulo.NoMod, HashModulo.Hundred)] HashModulo hashMod)
        {
            const long PageSize = 1L << PageSizeBits;

            using var session = store.NewSession<SpanByte, SpanByteAndMemory, Empty, ScanFunctions>(new ScanFunctions());
            var bContext = session.BasicContext;

            Random rng = new(101);

            Span<byte> keySpan = stackalloc byte[8];
            Span<byte> valueSpan = stackalloc byte[128];

            valueSpan.Fill((byte)'v');

            for (int i = 0; i < TotalRecords; i++)
            {
                _ = Utf8.TryWrite(keySpan, $"key_{i:0000}", out _);
                _ = Utf8.TryWrite(valueSpan, $"{i:0000}_", out int valueBytesWritten);

                var keySpanByte = SpanByte.FromPinnedSpan(keySpan);
                var valueSpanByte = SpanByte.FromPinnedSpan(valueSpan.Slice(0, rng.Next(valueBytesWritten + 1, valueSpan.Length))); // Make the record lengths random

                _ = bContext.Upsert(keySpanByte, valueSpanByte);
            }

            var scanCursorFuncs = new ScanCursorFuncs(store);

            // Normal operations
            var endAddresses = new long[] { store.Log.TailAddress, long.MaxValue };
            var counts = new long[] { 10, 100, long.MaxValue };

            long cursor = 0;
            for (var iAddr = 0; iAddr < endAddresses.Length; ++iAddr)
            {
                for (var iCount = 0; iCount < counts.Length; ++iCount)
                {
                    scanCursorFuncs.Initialize(verifyKeys: true);
                    while (session.ScanCursor(ref cursor, counts[iCount], scanCursorFuncs, endAddresses[iAddr]))
                        ;
                    ClassicAssert.AreEqual(TotalRecords, scanCursorFuncs.numRecords, $"count: {counts[iCount]}, endAddress {endAddresses[iAddr]}");
                    ClassicAssert.AreEqual(0, cursor, "Expected cursor to be 0, pt 1");
                }
            }

            // After FlushAndEvict, we will be doing pending IO. With collision chains, this means we may be returning colliding keys from in-memory
            // before the sequential keys from pending IO. Therefore we do not want to verify keys if we are causing collisions.
            store.Log.FlushAndEvict(wait: true);
            bool verifyKeys = hashMod == HashModulo.NoMod;

            // Scan and verify we see them all
            scanCursorFuncs.Initialize(verifyKeys);
            ClassicAssert.IsFalse(session.ScanCursor(ref cursor, long.MaxValue, scanCursorFuncs, long.MaxValue), "Expected scan to finish and return false, pt 1");
            ClassicAssert.AreEqual(TotalRecords, scanCursorFuncs.numRecords, "Unexpected count for all on-disk");
            ClassicAssert.AreEqual(0, cursor, "Expected cursor to be 0, pt 2");

            // Add another totalRecords, with keys incremented by totalRecords to remain distinct, and verify we see all keys.
            for (int i = TotalRecords; i < TotalRecords * 2; i++)
            {
                _ = Utf8.TryWrite(keySpan, $"key_{i:0000}", out _);
                _ = Utf8.TryWrite(valueSpan, $"{i:0000}_", out int valueBytesWritten);

                var keySpanByte = SpanByte.FromPinnedSpan(keySpan);
                var valueSpanByte = SpanByte.FromPinnedSpan(valueSpan.Slice(0, rng.Next(valueBytesWritten + 1, valueSpan.Length))); // Make the record lengths random

                _ = bContext.Upsert(keySpanByte, valueSpanByte);
            }
            scanCursorFuncs.Initialize(verifyKeys);
            ClassicAssert.IsFalse(session.ScanCursor(ref cursor, long.MaxValue, scanCursorFuncs, long.MaxValue), "Expected scan to finish and return false, pt 2");
            ClassicAssert.AreEqual(TotalRecords * 2, scanCursorFuncs.numRecords, "Unexpected count for on-disk + in-mem");
            ClassicAssert.AreEqual(0, cursor, "Expected cursor to be 0, pt 3");

            // Try an invalid cursor (not a multiple of 8) on-disk and verify we get one correct record. Use 3x page size to make sure page boundaries are tested.
            ClassicAssert.Greater(store.hlogBase.GetTailAddress(), PageSize * 10, "Need enough space to exercise this");
            scanCursorFuncs.Initialize(verifyKeys);
            cursor = store.hlogBase.BeginAddress - 1;
            do
            {
                ClassicAssert.IsTrue(session.ScanCursor(ref cursor, 1, scanCursorFuncs, long.MaxValue, validateCursor: true), "Expected scan to finish and return false, pt 1");
                cursor = scanCursorFuncs.lastAddress + scanCursorFuncs.lastRecordSize + 1;
            } while (cursor < PageSize * 3);

            // Now try an invalid cursor in-memory. First we have to read what's at the target start address (let's use HeadAddress) to find what the value is.
            SpanByte input = default;
            SpanByteAndMemory output = default;
            ReadOptions readOptions = default;
            var readStatus = bContext.ReadAtAddress(store.hlogBase.HeadAddress, ref input, ref output, ref readOptions, out _);
            ClassicAssert.IsTrue(readStatus.Found, $"Could not read at HeadAddress; {readStatus}");
            
            var outputKeySpan = output.AsReadOnlySpan();
            var outputKeyOrdinal = int.Parse(outputKeySpan.Slice(0, outputKeySpan.IndexOf((byte)'_')));
            output.Memory.Dispose();

            scanCursorFuncs.Initialize(verifyKeys);
            scanCursorFuncs.numRecords = outputKeyOrdinal;
            cursor = store.Log.HeadAddress + 1;
            do
            {
                ClassicAssert.IsTrue(session.ScanCursor(ref cursor, 1, scanCursorFuncs, long.MaxValue, validateCursor: true), "Expected scan to finish and return false, pt 1");
                cursor = scanCursorFuncs.lastAddress + scanCursorFuncs.lastRecordSize + 1;
            } while (cursor < store.hlogBase.HeadAddress + PageSize * 3);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void SpanByteScanCursorFilterTest([Values(HashModulo.NoMod, HashModulo.Hundred)] HashModulo hashMod)
        {
            using var session = store.NewSession<SpanByte, SpanByteAndMemory, Empty, ScanFunctions>(new ScanFunctions());
            var bContext = session.BasicContext;

            Random rng = new(101);
            
            Span<byte> keySpan = stackalloc byte[8];
            Span<byte> valueSpan = stackalloc byte[128];

            valueSpan.Fill((byte)'v');

            for (int i = 0; i < TotalRecords; i++)
            {
                _ = Utf8.TryWrite(keySpan, $"key_{i:0000}", out _);
                _ = Utf8.TryWrite(valueSpan, $"{i:0000}_", out int valueBytesWritten);

                var keySpanByte = SpanByte.FromPinnedSpan(keySpan);
                var valueSpanByte = SpanByte.FromPinnedSpan(valueSpan.Slice(0, rng.Next(valueBytesWritten + 1, valueSpan.Length))); // Make the record lengths random

                _ = bContext.Upsert(keySpanByte, valueSpanByte);
            }

            var scanCursorFuncs = new ScanCursorFuncs(store);

            long cursor = 0;
            scanCursorFuncs.Initialize(verifyKeys: false, k => k % 10 == 0);
            ClassicAssert.IsTrue(session.ScanCursor(ref cursor, 10, scanCursorFuncs, store.Log.TailAddress), "ScanCursor failed, pt 1");
            ClassicAssert.AreEqual(10, scanCursorFuncs.numRecords, "count at first 10");
            ClassicAssert.Greater(cursor, 0, "Expected cursor to be > 0, pt 1");

            // Now fake out the key verification to make it think we got all the previous keys; this ensures we are aligned as expected.
            scanCursorFuncs.Initialize(verifyKeys: false, k => true);
            scanCursorFuncs.numRecords = 91;   // (filter accepts: 0-9) * 10 + 1
            ClassicAssert.IsTrue(session.ScanCursor(ref cursor, 100, scanCursorFuncs, store.Log.TailAddress), "ScanCursor failed, pt 2");
            ClassicAssert.AreEqual(191, scanCursorFuncs.numRecords, "count at second 100");
            ClassicAssert.Greater(cursor, 0, "Expected cursor to be > 0, pt 1");
        }

        internal enum RCULocation { RCUNone, RCUBefore, RCUAfter };

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void SpanByteScanCursorWithRCUTest([Values(RCULocation.RCUBefore, RCULocation.RCUAfter)] RCULocation rcuLocation, [Values(HashModulo.NoMod, HashModulo.Hundred)] HashModulo hashMod)
        {
            using var session = store.NewSession<SpanByte, SpanByteAndMemory, Empty, ScanFunctions>(new ScanFunctions());
            var bContext = session.BasicContext;

            Random rng = new(101);

            Span<byte> keySpan = stackalloc byte[8];
            Span<byte> valueSpan = stackalloc byte[128];

            valueSpan.Fill((byte)'v');

            for (int i = 0; i < TotalRecords; i++)
            {
                _ = Utf8.TryWrite(keySpan, $"key_{i:0000}", out _);
                _ = Utf8.TryWrite(valueSpan, $"{i:0000}_", out int valueBytesWritten);

                var keySpanByte = SpanByte.FromPinnedSpan(keySpan);
                var valueSpanByte = SpanByte.FromPinnedSpan(valueSpan.Slice(0, rng.Next(valueBytesWritten + 1, valueSpan.Length))); // Make the record lengths random

                _ = bContext.Upsert(keySpanByte, valueSpanByte);
            }

            var scanCursorFuncs = new ScanCursorFuncs(store)
            {
                rcuLocation = rcuLocation,
                rcuRecord = TotalRecords - 10
            };

            long cursor = 0;

            if (rcuLocation == RCULocation.RCUBefore)
            {
                // RCU before we hit the record - verify we see it once; the original record is Sealed, and we see the one at the Tail.
                ClassicAssert.IsFalse(session.ScanCursor(ref cursor, long.MaxValue, scanCursorFuncs, long.MaxValue), "Expected scan to finish and return false, pt 1");
                ClassicAssert.AreEqual(TotalRecords, scanCursorFuncs.numRecords, "Unexpected count for RCU before we hit the scan value");
            }
            else
            {
                // RCU after we hit the record - verify we see it twice; once before we update, of course, then once again after it's added at the Tail.
                ClassicAssert.IsFalse(session.ScanCursor(ref cursor, long.MaxValue, scanCursorFuncs, long.MaxValue), "Expected scan to finish and return false, pt 1");
                ClassicAssert.AreEqual(TotalRecords + 1, scanCursorFuncs.numRecords, "Unexpected count for RCU after we hit the scan value");
            }
            ClassicAssert.IsTrue(scanCursorFuncs.rcuDone, "RCU was not done");
        }

        internal sealed class ScanCursorFuncs : IScanIteratorFunctions<SpanByte, SpanByte>
        {
            readonly TsavoriteKV<SpanByte, SpanByte, SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>> store;

            internal int numRecords;
            internal long lastAddress;
            internal int lastRecordSize;
            internal int rcuRecord, rcuOffset;
            internal RCULocation rcuLocation;
            internal bool rcuDone, verifyKeys;
            internal Func<int, bool> filter;

            internal ScanCursorFuncs(TsavoriteKV<SpanByte, SpanByte, SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>> store)
            {
                this.store = store;
                Initialize(verifyKeys: true);
            }

            internal void Initialize(bool verifyKeys) => Initialize(verifyKeys, k => true);

            internal void Initialize(bool verifyKeys, Func<int, bool> filter)
            {
                numRecords = lastRecordSize = 0;
                rcuRecord = -1;
                rcuOffset = 0;
                rcuLocation = RCULocation.RCUNone;
                rcuDone = false;
                this.verifyKeys = verifyKeys;
                this.filter = filter;
            }

            void CheckForRCU()
            {
                if (rcuLocation == RCULocation.RCUBefore && rcuRecord == numRecords + 1
                    || rcuLocation == RCULocation.RCUAfter && rcuRecord == numRecords - 1)
                {
                    // Must run this on another thread because we are epoch-protected on this one.
                    Task.Run(() =>
                    {
                        using var session = store.NewSession<SpanByte, SpanByteAndMemory, Empty, ScanFunctions>(new ScanFunctions());
                        var bContext = session.BasicContext;

                        Span<byte> keySpan = stackalloc byte[8];
                        Span<byte> valueSpan = stackalloc byte[225];

                        _ = Utf8.TryWrite(keySpan, $"key_{rcuRecord:0000}", out int keyBytesWritten);
                                                
                        valueSpan.Fill((byte)'v');
                        _ = Utf8.TryWrite(valueSpan, $"{rcuRecord:0000}_", out int valueBytesWritten);

                        var keySpanByte = SpanByte.FromPinnedSpan(keySpan);
                        var valueSpanByte = SpanByte.FromPinnedSpan(valueSpan); // Update the specified key with a longer value that requires RCU.

                        _ = bContext.Upsert(keySpanByte, valueSpanByte);
                       
                    }).Wait();

                    // If we RCU before Scan arrives at the record, then we won't see it and the values will be off by one (higher).
                    if (rcuLocation == RCULocation.RCUBefore)
                        rcuOffset = 1;
                    rcuDone = true;
                }
            }

            public bool ConcurrentReader(ref SpanByte key, ref SpanByte value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
            {
                var keySpan = key.AsReadOnlySpan();
                var kfield1 = int.Parse(keySpan.Slice(keySpan.IndexOf((byte)'_') + 1));

                cursorRecordResult = filter(kfield1) ? CursorRecordResult.Accept : CursorRecordResult.Skip;
                if (cursorRecordResult != CursorRecordResult.Accept)
                    return true;

                if (verifyKeys)
                {
                    if (rcuLocation != RCULocation.RCUNone && numRecords == TotalRecords - rcuOffset)
                        ClassicAssert.AreEqual(rcuRecord, kfield1, "Expected to find the rcuRecord value at end of RCU-testing enumeration");
                    else
                        ClassicAssert.AreEqual(numRecords + rcuOffset, kfield1, "Mismatched key field on Scan");
                }
                ClassicAssert.Greater(recordMetadata.Address, 0);

                lastAddress = recordMetadata.Address;
                lastRecordSize = RecordInfo.GetLength() + RoundUp(key.TotalSize, 8) + RoundUp(value.TotalSize, 8);

                CheckForRCU();
                ++numRecords;   // Do this *after* RCU
                return true;
            }

            public void OnException(Exception exception, long numberOfRecords)
                => Assert.Fail($"Unexpected exception at {numberOfRecords} records: {exception.Message}");

            public bool OnStart(long beginAddress, long endAddress) => true;

            public void OnStop(bool completed, long numberOfRecords) { }

            public bool SingleReader(ref SpanByte key, ref SpanByte value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                => ConcurrentReader(ref key, ref value, recordMetadata, numberOfRecords, out cursorRecordResult);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void SpanByteJumpToBeginAddressTest()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            using var log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "test.log"), deleteOnClose: true);

            store = new(new()
            {
                IndexSize = 1L << 26,
                LogDevice = log,
                MemorySize = 1L << 20,
                PageSize = 1L << PageSizeBits
            }, StoreFunctions<SpanByte, SpanByte>.Create(new SpanByteComparerModulo(0), SpanByteRecordDisposer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            using var session = store.NewSession<SpanByte, SpanByteAndMemory, Empty, SpanByteFunctions<Empty>>(new SpanByteFunctions<Empty>());
            var bContext = session.BasicContext;

            const int numRecords = 200;
            const int numTailRecords = 10;

            Span<byte> keyValueSpan = stackalloc byte[8];

            long shiftBeginAddressTo = 0;
            int shiftToKey = 0;
            for (int i = 0; i < numRecords; i++)
            {
                if (i == numRecords - numTailRecords)
                {
                    shiftBeginAddressTo = store.Log.TailAddress;
                    shiftToKey = i;
                }

                _ = i.TryFormat(keyValueSpan, out int keyValueBytesWritten);

                var keySpanByte = SpanByte.FromPinnedSpan(keyValueSpan.Slice(0, keyValueBytesWritten));
                var valueSpanByte = SpanByte.FromPinnedSpan(keyValueSpan.Slice(0, keyValueBytesWritten));

                _ = bContext.Upsert(keySpanByte, valueSpanByte);
            }

            using var iter = store.Log.Scan(store.Log.HeadAddress, store.Log.TailAddress);

            for (int i = 0; i < 100; ++i)
            {
                ClassicAssert.IsTrue(iter.GetNext(out var recordInfo));
                ClassicAssert.AreEqual(i, int.Parse(iter.GetKey().AsReadOnlySpan()));
                ClassicAssert.AreEqual(i, int.Parse(iter.GetValue().AsReadOnlySpan()));
            }

            store.Log.ShiftBeginAddress(shiftBeginAddressTo);

            for (int i = 0; i < numTailRecords; ++i)
            {
                ClassicAssert.IsTrue(iter.GetNext(out var recordInfo));
                if (i == 0)
                    ClassicAssert.AreEqual(store.Log.BeginAddress, iter.CurrentAddress);
                var expectedKey = numRecords - numTailRecords + i;
                ClassicAssert.AreEqual(expectedKey, int.Parse(iter.GetKey().AsReadOnlySpan()));
                ClassicAssert.AreEqual(expectedKey, int.Parse(iter.GetValue().AsReadOnlySpan()));
            }
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        [Category(IteratorCategory)]
        [Category(SmokeTestCategory)]
        public void SpanByteIterationPendingCollisionTest([Values(HashModulo.Hundred)] HashModulo hashMod)
        {
            using var session = store.NewSession<SpanByte, int[], Empty, VLVectorFunctions>(new VLVectorFunctions());
            var bContext = session.BasicContext;
            IterationCollisionTestFunctions scanIteratorFunctions = new();

            const int totalRecords = 2000;
            var start = store.Log.TailAddress;

            // Note: We only have a single value element; we are not exercising the "Variable Length" aspect here.
            Span<long> keySpan = stackalloc long[1], valueSpan = stackalloc long[1];
            SpanByte key = keySpan.AsSpanByte(), value = valueSpan.AsSpanByte();

            // Initial population
            for (int ii = 0; ii < totalRecords; ii++)
            {
                keySpan[0] = valueSpan[0] = ii;
                _ = bContext.Upsert(ref key, ref value);
            }

            // Evict so we can test the pending scan push
            store.Log.FlushAndEvict(wait: true);

            long cursor = 0;
            // Currently this returns false because there are some still-pending records when ScanLookup's GetNext loop ends (2000 is not an even multiple
            // of 256, which is the CompletePending block size). If this returns true, it means the CompletePending block fired on the last valid record.
            ClassicAssert.IsFalse(session.ScanCursor(ref cursor, totalRecords, scanIteratorFunctions), $"ScanCursor returned true even though all {scanIteratorFunctions.keys.Count} records were returned");
            ClassicAssert.AreEqual(totalRecords, scanIteratorFunctions.keys.Count);
        }
    }
}