// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using NUnit.Framework;
using Tsavorite.core;
using static Tsavorite.core.Utility;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test
{
    [TestFixture]
    internal class SpanByteLogScanTests
    {
        private TsavoriteKV<SpanByte, SpanByte> store;
        private IDevice log;
        const int totalRecords = 2000;
        const int PageSizeBits = 15;

        struct SpanByteComparerModulo : ITsavoriteEqualityComparer<SpanByte>
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

        [SetUp]
        public void Setup()
        {
            ITsavoriteEqualityComparer<SpanByte> comparer = null;
            foreach (var arg in TestContext.CurrentContext.Test.Arguments)
            {
                if (arg is HashModulo mod && mod == HashModulo.Hundred)
                {
                    comparer = new SpanByteComparerModulo(100);
                    continue;
                }
            }

            DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(MethodTestDir + "/test.log", deleteOnClose: true);
            store = new TsavoriteKV<SpanByte, SpanByte>
                (1L << 20, new LogSettings { LogDevice = log, MemorySizeBits = 25, PageSizeBits = PageSizeBits }, concurrencyControlMode: ConcurrencyControlMode.None, comparer: comparer);
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

            Random rng = new(101);

            for (int i = 0; i < totalRecords; i++)
            {
                var valueFill = new string('x', rng.Next(120));  // Make the record lengths random
                var key = MemoryMarshal.Cast<char, byte>($"key_{i}".AsSpan());
                var value = MemoryMarshal.Cast<char, byte>($"v{valueFill}_{i}".AsSpan());
                session.Upsert(key, value);
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
                    Assert.AreEqual(totalRecords, scanCursorFuncs.numRecords, $"count: {counts[iCount]}, endAddress {endAddresses[iAddr]}");
                    Assert.AreEqual(0, cursor, "Expected cursor to be 0, pt 1");
                }
            }

            // After FlushAndEvict, we will be doing pending IO. With collision chains, this means we may be returning colliding keys from in-memory
            // before the sequential keys from pending IO. Therefore we do not want to verify keys if we are causing collisions.
            store.Log.FlushAndEvict(wait: true);
            bool verifyKeys = hashMod == HashModulo.NoMod;

            // Scan and verify we see them all
            scanCursorFuncs.Initialize(verifyKeys);
            Assert.IsFalse(session.ScanCursor(ref cursor, long.MaxValue, scanCursorFuncs, long.MaxValue), "Expected scan to finish and return false, pt 1");
            Assert.AreEqual(totalRecords, scanCursorFuncs.numRecords, "Unexpected count for all on-disk");
            Assert.AreEqual(0, cursor, "Expected cursor to be 0, pt 2");

            // Add another totalRecords, with keys incremented by totalRecords to remain distinct, and verify we see all keys.
            for (int i = 0; i < totalRecords; i++)
            {
                var valueFill = new string('x', rng.Next(120));  // Make the record lengths random
                var key = MemoryMarshal.Cast<char, byte>($"key_{i + totalRecords}".AsSpan());
                var value = MemoryMarshal.Cast<char, byte>($"v{valueFill}_{i + totalRecords}".AsSpan());
                session.Upsert(key, value);
            }
            scanCursorFuncs.Initialize(verifyKeys);
            Assert.IsFalse(session.ScanCursor(ref cursor, long.MaxValue, scanCursorFuncs, long.MaxValue), "Expected scan to finish and return false, pt 2");
            Assert.AreEqual(totalRecords * 2, scanCursorFuncs.numRecords, "Unexpected count for on-disk + in-mem");
            Assert.AreEqual(0, cursor, "Expected cursor to be 0, pt 3");

            // Try an invalid cursor (not a multiple of 8) on-disk and verify we get one correct record. Use 3x page size to make sure page boundaries are tested.
            Assert.Greater(store.hlog.GetTailAddress(), PageSize * 10, "Need enough space to exercise this");
            scanCursorFuncs.Initialize(verifyKeys);
            cursor = store.hlog.BeginAddress - 1;
            do
            {
                Assert.IsTrue(session.ScanCursor(ref cursor, 1, scanCursorFuncs, long.MaxValue, validateCursor: true), "Expected scan to finish and return false, pt 1");
                cursor = scanCursorFuncs.lastAddress + scanCursorFuncs.lastRecordSize + 1;
            } while (cursor < PageSize * 3);

            // Now try an invalid cursor in-memory. First we have to read what's at the target start address (let's use HeadAddress) to find what the value is.
            SpanByte input = default;
            SpanByteAndMemory output = default;
            ReadOptions readOptions = default;
            var readStatus = session.ReadAtAddress(store.hlog.HeadAddress, ref input, ref output, ref readOptions, out _);
            Assert.IsTrue(readStatus.Found, $"Could not read at HeadAddress; {readStatus}");
            var keyString = new string(MemoryMarshal.Cast<byte, char>(output.Memory.Memory.Span));
            var keyOrdinal = int.Parse(keyString.Substring(keyString.IndexOf('_') + 1));
            output.Memory.Dispose();

            scanCursorFuncs.Initialize(verifyKeys);
            scanCursorFuncs.numRecords = keyOrdinal;
            cursor = store.Log.HeadAddress + 1;
            do
            {
                Assert.IsTrue(session.ScanCursor(ref cursor, 1, scanCursorFuncs, long.MaxValue, validateCursor: true), "Expected scan to finish and return false, pt 1");
                cursor = scanCursorFuncs.lastAddress + scanCursorFuncs.lastRecordSize + 1;
            } while (cursor < store.hlog.HeadAddress + PageSize * 3);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]

        public void SpanByteScanCursorFilterTest([Values(HashModulo.NoMod, HashModulo.Hundred)] HashModulo hashMod)
        {
            using var session = store.NewSession<SpanByte, SpanByteAndMemory, Empty, ScanFunctions>(new ScanFunctions());

            Random rng = new(101);

            for (int i = 0; i < totalRecords; i++)
            {
                var valueFill = new string('x', rng.Next(120));  // Make the record lengths random
                var key = MemoryMarshal.Cast<char, byte>($"key_{i}".AsSpan());
                var value = MemoryMarshal.Cast<char, byte>($"v{valueFill}_{i}".AsSpan());
                session.Upsert(key, value);
            }

            var scanCursorFuncs = new ScanCursorFuncs(store);

            long cursor = 0;
            scanCursorFuncs.Initialize(verifyKeys: false, k => k % 10 == 0);
            Assert.IsTrue(session.ScanCursor(ref cursor, 10, scanCursorFuncs, store.Log.TailAddress), "ScanCursor failed, pt 1");
            Assert.AreEqual(10, scanCursorFuncs.numRecords, "count at first 10");
            Assert.Greater(cursor, 0, "Expected cursor to be > 0, pt 1");

            // Now fake out the key verification to make it think we got all the previous keys; this ensures we are aligned as expected.
            scanCursorFuncs.Initialize(verifyKeys: false, k => true);
            scanCursorFuncs.numRecords = 91;   // (filter accepts: 0-9) * 10 + 1
            Assert.IsTrue(session.ScanCursor(ref cursor, 100, scanCursorFuncs, store.Log.TailAddress), "ScanCursor failed, pt 2");
            Assert.AreEqual(191, scanCursorFuncs.numRecords, "count at second 100");
            Assert.Greater(cursor, 0, "Expected cursor to be > 0, pt 1");
        }

        internal enum RCULocation { RCUNone, RCUBefore, RCUAfter };

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Style", "IDE0060:Remove unused parameter", Justification = "hashMod is used by Setup")]
        public void SpanByteScanCursorWithRCUTest([Values(RCULocation.RCUBefore, RCULocation.RCUAfter)] RCULocation rcuLocation, [Values(HashModulo.NoMod, HashModulo.Hundred)] HashModulo hashMod)
        {
            using var session = store.NewSession<SpanByte, SpanByteAndMemory, Empty, ScanFunctions>(new ScanFunctions());

            Random rng = new(101);

            for (int i = 0; i < totalRecords; i++)
            {
                var valueFill = new string('x', rng.Next(120));  // Make the record lengths random
                var key = MemoryMarshal.Cast<char, byte>($"key_{i}".AsSpan());
                var value = MemoryMarshal.Cast<char, byte>($"v{valueFill}_{i}".AsSpan());
                session.Upsert(key, value);
            }

            var scanCursorFuncs = new ScanCursorFuncs(store)
            {
                rcuLocation = rcuLocation,
                rcuRecord = totalRecords - 10
            };

            long cursor = 0;

            if (rcuLocation == RCULocation.RCUBefore)
            {
                // RCU before we hit the record - verify we see it once; the original record is Sealed, and we see the one at the Tail.
                Assert.IsFalse(session.ScanCursor(ref cursor, long.MaxValue, scanCursorFuncs, long.MaxValue), "Expected scan to finish and return false, pt 1");
                Assert.AreEqual(totalRecords, scanCursorFuncs.numRecords, "Unexpected count for RCU before we hit the scan value");
            }
            else
            {
                // RCU after we hit the record - verify we see it twice; once before we update, of course, then once again after it's added at the Tail.
                Assert.IsFalse(session.ScanCursor(ref cursor, long.MaxValue, scanCursorFuncs, long.MaxValue), "Expected scan to finish and return false, pt 1");
                Assert.AreEqual(totalRecords + 1, scanCursorFuncs.numRecords, "Unexpected count for RCU after we hit the scan value");
            }
            Assert.IsTrue(scanCursorFuncs.rcuDone, "RCU was not done");
        }

        internal sealed class ScanCursorFuncs : IScanIteratorFunctions<SpanByte, SpanByte>
        {
            readonly TsavoriteKV<SpanByte, SpanByte> store;

            internal int numRecords;
            internal long lastAddress;
            internal int lastRecordSize;
            internal int rcuRecord, rcuOffset;
            internal RCULocation rcuLocation;
            internal bool rcuDone, verifyKeys;
            internal Func<int, bool> filter;

            internal ScanCursorFuncs(TsavoriteKV<SpanByte, SpanByte> store)
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

                        var valueFill = new string('x', 220);   // Update the specified key with a longer value that requires RCU.
                        var key = MemoryMarshal.Cast<char, byte>($"key_{rcuRecord}".AsSpan());
                        var value = MemoryMarshal.Cast<char, byte>($"v{valueFill}_{rcuRecord}".AsSpan());
                        session.Upsert(key, value);
                    }).Wait();

                    // If we RCU before Scan arrives at the record, then we won't see it and the values will be off by one (higher).
                    if (rcuLocation == RCULocation.RCUBefore)
                        rcuOffset = 1;
                    rcuDone = true;
                }
            }

            public bool ConcurrentReader(ref SpanByte key, ref SpanByte value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
            {
                var keyString = new string(MemoryMarshal.Cast<byte, char>(key.AsReadOnlySpan()));
                var kfield1 = int.Parse(keyString.Substring(keyString.IndexOf('_') + 1));

                cursorRecordResult = filter(kfield1) ? CursorRecordResult.Accept : CursorRecordResult.Skip;
                if (cursorRecordResult != CursorRecordResult.Accept)
                    return true;

                if (verifyKeys)
                {
                    if (rcuLocation != RCULocation.RCUNone && numRecords == totalRecords - rcuOffset)
                        Assert.AreEqual(rcuRecord, kfield1, "Expected to find the rcuRecord value at end of RCU-testing enumeration");
                    else
                        Assert.AreEqual(numRecords + rcuOffset, kfield1, "Mismatched key field on Scan");
                }
                Assert.Greater(recordMetadata.Address, 0);

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

        public unsafe void SpanByteJumpToBeginAddressTest()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            using var log = Devices.CreateLogDevice(MethodTestDir + "/test.log", deleteOnClose: true);
            using var store = new TsavoriteKV<SpanByte, SpanByte>
                (1L << 20, new LogSettings { LogDevice = log, MemorySizeBits = 20, PageSizeBits = 15 }, concurrencyControlMode: ConcurrencyControlMode.None);
            using var session = store.NewSession<SpanByte, SpanByteAndMemory, Empty, SpanByteFunctions<Empty>>(new SpanByteFunctions<Empty>());

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
                session.Upsert(key, value);
            }

            using var iter = store.Log.Scan(store.Log.HeadAddress, store.Log.TailAddress);

            for (int i = 0; i < 100; ++i)
            {
                Assert.IsTrue(iter.GetNext(out var recordInfo));
                Assert.AreEqual(i, int.Parse(MemoryMarshal.Cast<byte, char>(iter.GetKey().AsSpan())));
                Assert.AreEqual(i, int.Parse(MemoryMarshal.Cast<byte, char>(iter.GetValue().AsSpan())));
            }

            store.Log.ShiftBeginAddress(shiftBeginAddressTo);

            for (int i = 0; i < numTailRecords; ++i)
            {
                Assert.IsTrue(iter.GetNext(out var recordInfo));
                if (i == 0)
                    Assert.AreEqual(store.Log.BeginAddress, iter.CurrentAddress);
                var expectedKey = numRecords - numTailRecords + i;
                Assert.AreEqual(expectedKey, int.Parse(MemoryMarshal.Cast<byte, char>(iter.GetKey().AsSpan())));
                Assert.AreEqual(expectedKey, int.Parse(MemoryMarshal.Cast<byte, char>(iter.GetValue().AsSpan())));
            }
        }
    }
}