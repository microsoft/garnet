// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Allure.NUnit;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.SpanByteIterationTests;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.spanbyte
{
    // Must be in a separate block so the "using SpanByteStoreFunctions" is the first line in its namespace declaration.
    struct SpanByteComparerModulo : IKeyComparer
    {
        readonly long mod;

        internal SpanByteComparerModulo(long mod) => this.mod = mod;

        public readonly bool Equals(ReadOnlySpan<byte> k1, ReadOnlySpan<byte> k2) => SpanByteComparer.StaticEquals(k1, k2);

        // Force collisions to create a chain
        public readonly long GetHashCode64(ReadOnlySpan<byte> k)
        {
            long hash = SpanByteComparer.StaticGetHashCode64(k);
            return mod > 0 ? hash % mod : hash;
        }
    }
}

namespace Tsavorite.test.spanbyte
{
    using SpanByteStoreFunctions = StoreFunctions<SpanByteComparerModulo, SpanByteRecordDisposer>;

    [AllureNUnit]
    [TestFixture]
    internal class SpanByteLogScanTests : AllureTestBase
    {
        private TsavoriteKV<SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>> store;
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
            }, StoreFunctions.Create(comparer, SpanByteRecordDisposer.Instance)
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

            public override bool InitialWriter(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref PinnedSpanByte input, ReadOnlySpan<byte> src, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo)
            {
                insertedAddress = upsertInfo.Address;
                return base.InitialWriter(ref dstLogRecord, in sizeInfo, ref input, src, ref output, ref upsertInfo);
            }
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void SpanByteScanCursorTest([Values(HashModulo.NoMod, HashModulo.Hundred)] HashModulo hashMod)
        {
            const long PageSize = 1L << PageSizeBits;

            using var session = store.NewSession<PinnedSpanByte, SpanByteAndMemory, Empty, ScanFunctions>(new ScanFunctions());
            var bContext = session.BasicContext;

            Random rng = new(101);

            for (int i = 0; i < TotalRecords; i++)
            {
                var valueFill = new string('x', rng.Next(120));  // Make the record lengths random
                var key = MemoryMarshal.Cast<char, byte>($"key_{i}".AsSpan());
                var value = MemoryMarshal.Cast<char, byte>($"v{valueFill}_{i}".AsSpan());
                _ = bContext.Upsert(key, value);
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
            for (int i = 0; i < TotalRecords; i++)
            {
                var valueFill = new string('x', rng.Next(120));  // Make the record lengths random
                var key = MemoryMarshal.Cast<char, byte>($"key_{i + TotalRecords}".AsSpan());
                var value = MemoryMarshal.Cast<char, byte>($"v{valueFill}_{i + TotalRecords}".AsSpan());
                _ = bContext.Upsert(key, value);
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
                ClassicAssert.IsTrue(session.ScanCursor(ref cursor, 1, scanCursorFuncs, long.MaxValue, validateCursor: true), "Expected scan to finish and return false, pt 3");
                Assert.That(cursor, Is.EqualTo(scanCursorFuncs.lastAddress + scanCursorFuncs.lastRecordSize));
                cursor += 1;
            } while (cursor < PageSize * 3);

            // Now try an invalid cursor in-memory. First we have to read what's at the target start address (let's use HeadAddress) to find what the value is.
            PinnedSpanByte input = default;
            SpanByteAndMemory output = default;
            ReadOptions readOptions = default;
            var readStatus = bContext.ReadAtAddress(store.hlogBase.HeadAddress, ref input, ref output, ref readOptions, out _);
            ClassicAssert.IsTrue(readStatus.Found, $"Could not read at HeadAddress; {readStatus}");
            var keyString = new string(MemoryMarshal.Cast<byte, char>(output.ReadOnlySpan));
            var keyOrdinal = int.Parse(keyString.Substring(keyString.IndexOf('_') + 1));
            output.Memory.Dispose();

            scanCursorFuncs.Initialize(verifyKeys);
            scanCursorFuncs.numRecords = keyOrdinal;
            cursor = store.Log.HeadAddress + 1;
            do
            {
                ClassicAssert.IsTrue(session.ScanCursor(ref cursor, 1, scanCursorFuncs, long.MaxValue, validateCursor: true), "Expected scan to finish and return false, pt 1");
                Assert.That(cursor, Is.EqualTo(scanCursorFuncs.lastAddress + scanCursorFuncs.lastRecordSize));
                cursor += 1;
            } while (cursor < store.hlogBase.HeadAddress + PageSize * 3);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void SpanByteScanCursorFilterTest([Values(HashModulo.NoMod, HashModulo.Hundred)] HashModulo hashMod)
        {
            using var session = store.NewSession<PinnedSpanByte, SpanByteAndMemory, Empty, ScanFunctions>(new ScanFunctions());
            var bContext = session.BasicContext;

            Random rng = new(101);

            for (int i = 0; i < TotalRecords; i++)
            {
                var valueFill = new string('x', rng.Next(120));  // Make the record lengths random
                var key = MemoryMarshal.Cast<char, byte>($"key_{i}".AsSpan());
                var value = MemoryMarshal.Cast<char, byte>($"v{valueFill}_{i}".AsSpan());
                _ = bContext.Upsert(key, value);
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
            using var session = store.NewSession<PinnedSpanByte, SpanByteAndMemory, Empty, ScanFunctions>(new ScanFunctions());
            var bContext = session.BasicContext;

            Random rng = new(101);

            for (int i = 0; i < TotalRecords; i++)
            {
                var valueFill = new string('x', rng.Next(120));  // Make the record lengths random
                var key = MemoryMarshal.Cast<char, byte>($"key_{i}".AsSpan());
                var value = MemoryMarshal.Cast<char, byte>($"v{valueFill}_{i}".AsSpan());
                _ = bContext.Upsert(key, value);
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

        internal sealed class ScanCursorFuncs : IScanIteratorFunctions
        {
            readonly TsavoriteKV<SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>> store;

            internal int numRecords;
            internal long lastAddress;
            internal int lastRecordSize;
            internal int rcuRecord, rcuOffset;
            internal RCULocation rcuLocation;
            internal bool rcuDone, verifyKeys;
            internal Func<int, bool> filter;

            internal ScanCursorFuncs(TsavoriteKV<SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>> store)
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
                        using var session = store.NewSession<PinnedSpanByte, SpanByteAndMemory, Empty, ScanFunctions>(new ScanFunctions());
                        var bContext = session.BasicContext;

                        var valueFill = new string('x', 220);   // Update the specified key with a longer value that requires RCU.
                        var key = MemoryMarshal.Cast<char, byte>($"key_{rcuRecord}".AsSpan());
                        var value = MemoryMarshal.Cast<char, byte>($"v{valueFill}_{rcuRecord}".AsSpan());
                        _ = bContext.Upsert(key, value);
                    }).Wait();

                    // If we RCU before Scan arrives at the record, then we won't see it and the values will be off by one (higher).
                    if (rcuLocation == RCULocation.RCUBefore)
                        rcuOffset = 1;
                    rcuDone = true;
                }
            }

            public bool Reader<TSourceLogRecord>(in TSourceLogRecord logRecord, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                where TSourceLogRecord : ISourceLogRecord
            {
                var keyString = new string(MemoryMarshal.Cast<byte, char>(logRecord.Key));
                var kfield1 = int.Parse(keyString.Substring(keyString.IndexOf('_') + 1));

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
                lastRecordSize = logRecord.AllocatedSize;

                CheckForRCU();
                ++numRecords;   // Do this *after* RCU
                return true;
            }

            public void OnException(Exception exception, long numberOfRecords)
                => Assert.Fail($"Unexpected exception at {numberOfRecords} records: {exception.Message}");

            public bool OnStart(long beginAddress, long endAddress) => true;

            public void OnStop(bool completed, long numberOfRecords) { }
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
            }, StoreFunctions.Create(new SpanByteComparerModulo(0), SpanByteRecordDisposer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            using var session = store.NewSession<PinnedSpanByte, SpanByteAndMemory, Empty, SpanByteFunctions<Empty>>(new SpanByteFunctions<Empty>());
            var bContext = session.BasicContext;

            const int numRecords = 200;
            const int numTailRecords = 10;
            long shiftBeginAddressTo = 0;
            int shiftToKey = 0;
            for (int i = 0; i < numRecords; i++)
            {
                if (i == numRecords - numTailRecords)
                {
                    shiftBeginAddressTo = store.Log.TailAddress;
                    shiftToKey = i;
                }

                var key = MemoryMarshal.Cast<char, byte>($"{i}".AsSpan());
                var value = MemoryMarshal.Cast<char, byte>($"{i}".AsSpan());
                _ = bContext.Upsert(key, value);
            }

            using var iter = store.Log.Scan(store.Log.HeadAddress, store.Log.TailAddress);

            for (int i = 0; i < 100; ++i)
            {
                ClassicAssert.IsTrue(iter.GetNext());
                ClassicAssert.AreEqual(i, int.Parse(MemoryMarshal.Cast<byte, char>(iter.Key)));
                ClassicAssert.AreEqual(i, int.Parse(MemoryMarshal.Cast<byte, char>(iter.ValueSpan)));
            }

            store.Log.ShiftBeginAddress(shiftBeginAddressTo);

            for (int i = 0; i < numTailRecords; ++i)
            {
                ClassicAssert.IsTrue(iter.GetNext());
                if (i == 0)
                    ClassicAssert.AreEqual(store.Log.BeginAddress, iter.CurrentAddress);
                var expectedKey = numRecords - numTailRecords + i;
                ClassicAssert.AreEqual(expectedKey, int.Parse(MemoryMarshal.Cast<byte, char>(iter.Key)));
                ClassicAssert.AreEqual(expectedKey, int.Parse(MemoryMarshal.Cast<byte, char>(iter.ValueSpan)));
            }
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        [Category(IteratorCategory)]
        [Category(SmokeTestCategory)]
#pragma warning disable IDE0060 // Remove unused parameter (hashMod is used by Setup)
        public void SpanByteIterationPendingCollisionTest([Values(HashModulo.Hundred)] HashModulo hashMod)
#pragma warning restore IDE0060 // Remove unused parameter
        {
            using var session = store.NewSession<PinnedSpanByte, int[], Empty, VLVectorFunctions>(new VLVectorFunctions());
            var bContext = session.BasicContext;
            IterationCollisionTestFunctions scanIteratorFunctions = new();

            const int totalRecords = 2000;
            var start = store.Log.TailAddress;

            // Note: We only have a single value element; we are not exercising the "Variable Length" aspect here.
            long key, value;

            // Initial population
            for (int ii = 0; ii < totalRecords; ii++)
            {
                key = value = ii;
                _ = bContext.Upsert(SpanByte.FromPinnedVariable(ref key), SpanByte.FromPinnedVariable(ref value));
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