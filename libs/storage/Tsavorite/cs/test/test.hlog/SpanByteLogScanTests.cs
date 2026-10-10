// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
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

        public readonly bool Equals<TFirstKey, TSecondKey>(TFirstKey k1, TSecondKey k2)
            where TFirstKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            where TSecondKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            => SpanByteComparer.StaticEquals(k1.KeyBytes, k2.KeyBytes);

        // Force collisions to create a chain
        public readonly long GetHashCode64<TKey>(TKey k)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
        {
            long hash = SpanByteComparer.StaticGetHashCode64(k.KeyBytes);
            return mod > 0 ? hash % mod : hash;
        }
    }
}

namespace Tsavorite.test.spanbyte
{
    using SpanByteStoreFunctions = StoreFunctions<SpanByteComparerModulo, SpanByteRecordTriggers>;
    [TestFixture]
    internal class SpanByteLogScanTests : TestBase
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
                LogMemorySize = 1L << 25,
                PageSize = 1L << PageSizeBits
            }, StoreFunctions.Create(comparer, SpanByteRecordTriggers.Instance)
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
            OnTearDown();
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
        public unsafe void SpanByteScanCursorTest([Values(HashModulo.NoMod, HashModulo.Hundred)] HashModulo hashMod)
        {
            const long PageSize = 1L << PageSizeBits;

            using var session = store.NewSession<TestSpanByteKey, PinnedSpanByte, SpanByteAndMemory, Empty, ScanFunctions>(new ScanFunctions());
            var bContext = session.BasicContext;

            Random rng = new(101);

            for (int i = 0; i < TotalRecords; i++)
            {
                var valueFill = new string('x', rng.Next(120));  // Make the record lengths random
                var key = MemoryMarshal.Cast<char, byte>($"key_{i}".AsSpan());

                var value = MemoryMarshal.Cast<char, byte>($"v{valueFill}_{i}".AsSpan());

                fixed (byte* keyPtr = key)
                {
                    _ = bContext.Upsert(TestSpanByteKey.FromPointer(keyPtr, key.Length), value);
                }
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
                fixed (byte* keyPtr = key)
                {
                    _ = bContext.Upsert(TestSpanByteKey.FromPointer(keyPtr, key.Length), value);
                }
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
        public unsafe void SpanByteScanCursorFilterTest([Values(HashModulo.NoMod, HashModulo.Hundred)] HashModulo hashMod)
        {
            using var session = store.NewSession<TestSpanByteKey, PinnedSpanByte, SpanByteAndMemory, Empty, ScanFunctions>(new ScanFunctions());
            var bContext = session.BasicContext;

            Random rng = new(101);

            for (int i = 0; i < TotalRecords; i++)
            {
                var valueFill = new string('x', rng.Next(120));  // Make the record lengths random
                var key = MemoryMarshal.Cast<char, byte>($"key_{i}".AsSpan());
                var value = MemoryMarshal.Cast<char, byte>($"v{valueFill}_{i}".AsSpan());
                fixed (byte* keyPtr = key)
                {
                    _ = bContext.Upsert(TestSpanByteKey.FromPointer(keyPtr, key.Length), value);
                }
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
        public unsafe void SpanByteScanCursorWithRCUTest([Values(RCULocation.RCUBefore, RCULocation.RCUAfter)] RCULocation rcuLocation, [Values(HashModulo.NoMod, HashModulo.Hundred)] HashModulo hashMod)
        {
            using var session = store.NewSession<TestSpanByteKey, PinnedSpanByte, SpanByteAndMemory, Empty, ScanFunctions>(new ScanFunctions());
            var bContext = session.BasicContext;

            Random rng = new(101);

            for (int i = 0; i < TotalRecords; i++)
            {
                var valueFill = new string('x', rng.Next(120));  // Make the record lengths random
                var key = MemoryMarshal.Cast<char, byte>($"key_{i}".AsSpan());
                var value = MemoryMarshal.Cast<char, byte>($"v{valueFill}_{i}".AsSpan());
                fixed (byte* keyPtr = key)
                {
                    _ = bContext.Upsert(TestSpanByteKey.FromPointer(keyPtr, key.Length), value);
                }
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

            unsafe void CheckForRCU()
            {
                if (rcuLocation == RCULocation.RCUBefore && rcuRecord == numRecords + 1
                    || rcuLocation == RCULocation.RCUAfter && rcuRecord == numRecords - 1)
                {
                    // Must run this on another thread because we are epoch-protected on this one.
                    Task.Run(() =>
                    {
                        using var session = store.NewSession<TestSpanByteKey, PinnedSpanByte, SpanByteAndMemory, Empty, ScanFunctions>(new ScanFunctions());
                        var bContext = session.BasicContext;

                        var valueFill = new string('x', 220);   // Update the specified key with a longer value that requires RCU.
                        var key = MemoryMarshal.Cast<char, byte>($"key_{rcuRecord}".AsSpan());
                        var value = MemoryMarshal.Cast<char, byte>($"v{valueFill}_{rcuRecord}".AsSpan());
                        fixed (byte* keyPtr = key)
                        {
                            _ = bContext.Upsert(TestSpanByteKey.FromPointer(keyPtr, key.Length), value);
                        }
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

        public unsafe void SpanByteJumpToBeginAddressTest()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            using var log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "test.log"), deleteOnClose: true);

            // Dispose existing store as we create a new one in this test
            store?.Dispose();

            store = new(new()
            {
                IndexSize = 1L << 26,
                LogDevice = log,
                LogMemorySize = 1L << 20,
                PageSize = 1L << PageSizeBits
            }, StoreFunctions.Create(new SpanByteComparerModulo(0), SpanByteRecordTriggers.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            using var session = store.NewSession<TestSpanByteKey, PinnedSpanByte, SpanByteAndMemory, Empty, SpanByteFunctions<Empty>>(new SpanByteFunctions<Empty>());
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

                fixed (byte* keyPtr = key)
                {
                    _ = bContext.Upsert(TestSpanByteKey.FromPointer(keyPtr, key.Length), value);
                }
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
            using var session = store.NewSession<TestSpanByteKey, PinnedSpanByte, int[], Empty, VLVectorFunctions>(new VLVectorFunctions());
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
                _ = bContext.Upsert(TestSpanByteKey.FromPinnedSpan(SpanByte.FromPinnedVariable(ref key)), SpanByte.FromPinnedVariable(ref value));
            }

            // Evict so we can test the pending scan push
            store.Log.FlushAndEvict(wait: true);

            long cursor = 0;
            // Currently this returns false because there are some still-pending records when ScanLookup's GetNext loop ends (2000 is not an even multiple
            // of 256, which is the CompletePending block size). If this returns true, it means the CompletePending block fired on the last valid record.
            ClassicAssert.IsFalse(session.ScanCursor(ref cursor, totalRecords, scanIteratorFunctions), $"ScanCursor returned true even though all {scanIteratorFunctions.keys.Count} records were returned");
            ClassicAssert.AreEqual(totalRecords, scanIteratorFunctions.keys.Count);
        }

        // Values are large enough that a record spans several device sectors, so the boundary a scan end is aimed at
        // falls well inside the value rather than in a record header or trailing alignment padding.
        const int ScanEndTestRecords = 500;
        const int ScanEndTestValueLength = 2000;

        /// <summary>Value bytes are all nonzero and derived from the ordinal, so a record truncated at a sector boundary is visible as a run of zeros.</summary>
        static byte[] CreateScanEndTestValue(int ordinal)
        {
            var value = new byte[ScanEndTestValueLength];
            for (var ii = 0; ii < value.Length; ++ii)
                value[ii] = (byte)(1 + ((ordinal + ii) % 251));
            return value;
        }

        static ReadOnlySpan<byte> ScanEndTestKey(int ordinal) => MemoryMarshal.Cast<char, byte>($"key_{ordinal}".AsSpan());

        unsafe void PopulateForScanEndTest(BasicContext<TestSpanByteKey, PinnedSpanByte, SpanByteAndMemory, Empty, ScanFunctions,
                SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>> bContext)
        {
            for (var ii = 0; ii < ScanEndTestRecords; ++ii)
            {
                var key = ScanEndTestKey(ii);
                fixed (byte* keyPtr = key)
                    _ = bContext.Upsert(TestSpanByteKey.FromPointer(keyPtr, key.Length), CreateScanEndTestValue(ii));
            }
        }

        /// <summary>
        /// Find a record that a device-sector boundary runs through, so that ending a scan just inside the record makes the
        /// frame load stop in the middle of it. Scans to the tail, which never ends inside a record, so the key and value
        /// returned are the record's true content.
        /// </summary>
        /// <returns>The address of the record, or 0 if no such record was found</returns>
        long FindRecordStraddlingSectorBoundary(out byte[] key, out byte[] value, out long nextAddress)
        {
            var sectorSize = store.hlogBase.GetDeviceSectorSize();
            using var iter = store.Log.Scan(store.Log.BeginAddress, store.Log.TailAddress);
            while (iter.GetNext())
            {
                // The first sector boundary at or after the scan end we will use (CurrentAddress + 8).
                var sectorBoundary = (iter.CurrentAddress + 8 + sectorSize - 1) / sectorSize * sectorSize;

                // Require the boundary to fall before the end of the value, not in any trailing alignment padding, so that
                // truncating there necessarily damages the record. The record header is at least RecordInfo-sized, so
                // CurrentAddress + sizeof(RecordInfo) + key + value is a lower bound on where the value ends.
                if (sectorBoundary < iter.CurrentAddress + 8 + iter.Key.Length + iter.ValueSpan.Length)
                {
                    key = iter.Key.ToArray();
                    value = iter.ValueSpan.ToArray();
                    nextAddress = iter.NextAddress;
                    return iter.CurrentAddress;
                }
            }

            key = value = null;
            nextAddress = 0;
            return 0;
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        [Category(IteratorCategory)]
        [Category(SmokeTestCategory)]
        public void SpanByteScanEndInsideRecordTest()
        {
            using var session = store.NewSession<TestSpanByteKey, PinnedSpanByte, SpanByteAndMemory, Empty, ScanFunctions>(new ScanFunctions());
            PopulateForScanEndTest(session.BasicContext);

            var victimAddress = FindRecordStraddlingSectorBoundary(out var victimKey, out var victimValue, out var victimNextAddress);
            ClassicAssert.Greater(victimAddress, 0, "Did not find a record straddling a sector boundary");

            // End the scan inside the victim record. The record still starts below the end address, so the scan must return it whole.
            var scanEnd = victimAddress + 8;

            (byte[] key, byte[] value, long nextAddress) ScanToLastRecord()
            {
                byte[] lastKey = null, lastValue = null;
                long lastNextAddress = 0;
                using var iter = store.Log.Scan(store.Log.BeginAddress, scanEnd);
                while (iter.GetNext())
                {
                    lastKey = iter.Key.ToArray();
                    lastValue = iter.ValueSpan.ToArray();
                    lastNextAddress = iter.NextAddress;
                }
                return (lastKey, lastValue, lastNextAddress);
            }

            var inMemory = ScanToLastRecord();
            Assert.That(inMemory.key, Is.EqualTo(victimKey), "In-memory scan returned the wrong record");
            Assert.That(inMemory.value, Is.EqualTo(victimValue), "In-memory scan truncated the record");
            Assert.That(inMemory.nextAddress, Is.EqualTo(victimNextAddress), "In-memory scan did not advance past the whole record");

            store.Log.FlushAndEvict(wait: true);
            ClassicAssert.Greater(store.Log.HeadAddress, victimNextAddress, "Expected the victim record to be on disk");

            // The same call must return the same record now that it is read into a frame rather than read in place.
            var onDisk = ScanToLastRecord();
            Assert.That(onDisk.key, Is.EqualTo(victimKey), "On-disk scan returned the wrong record");
            Assert.That(onDisk.value, Is.EqualTo(victimValue), "On-disk scan truncated the record");
            Assert.That(onDisk.nextAddress, Is.EqualTo(victimNextAddress), "On-disk scan did not advance past the whole record");
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        [Category(IteratorCategory)]
        [Category("Compaction")]
        public unsafe void SpanByteCompactionEndInsideRecordTest([Values] CompactionType compactionType)
        {
            using var session = store.NewSession<TestSpanByteKey, PinnedSpanByte, SpanByteAndMemory, Empty, ScanFunctions>(new ScanFunctions());
            var bContext = session.BasicContext;
            PopulateForScanEndTest(bContext);

            var victimAddress = FindRecordStraddlingSectorBoundary(out _, out _, out _);
            ClassicAssert.Greater(victimAddress, 0, "Did not find a record straddling a sector boundary");

            store.Log.FlushAndEvict(wait: true);

            // Compaction takes its own record boundary from the scan's NextAddress, so an untilAddress inside a record is
            // expected. If the scan hands back that record truncated, the truncated copy goes to the tail and
            // ShiftBeginAddress drops the intact original.
            _ = session.Compact(victimAddress + 8, compactionType);
            store.Log.Truncate();

            for (var ii = 0; ii < ScanEndTestRecords; ++ii)
            {
                var key = ScanEndTestKey(ii);
                SpanByteAndMemory output = default;
                Status status;
                fixed (byte* keyPtr = key)
                    status = bContext.Read(TestSpanByteKey.FromPointer(keyPtr, key.Length), ref output);
                if (status.IsPending)
                {
                    _ = bContext.CompletePendingWithOutputs(out var completed, wait: true);
                    using (completed)
                    {
                        while (completed.Next())
                        {
                            status = completed.Current.Status;
                            output = completed.Current.Output;
                        }
                    }
                }

                ClassicAssert.IsTrue(status.Found, $"key_{ii} was not found after compaction");
                Assert.That(output.ReadOnlySpan.ToArray(), Is.EqualTo(CreateScanEndTestValue(ii)), $"key_{ii} has a damaged value after compaction");
                output.Memory?.Dispose();
            }
        }
    }
}