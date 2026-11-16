// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Threading.Tasks;
using NUnit.Framework;
using Tsavorite.core;
using static Tsavorite.core.Utility;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.recovery
{
    using InlineAllocator = SpanByteAllocator<StoreFunctions<LongKeyComparerModulo, SpanByteRecordTriggers>>;
    using InlineStoreFunctions = StoreFunctions<LongKeyComparerModulo, SpanByteRecordTriggers>;

    /// <summary>
    /// The inline-value counterpart of <see cref="FlushBoundarySectorRecoveryTests"/>. A FoldOver checkpoint's flush usually ends
    /// mid-sector, and the next flush writes that sector again. Superseding a record there elides it, or clears its optional fields, in
    /// the live page, so the next flush must write the bytes below its start as the checkpoint made them durable: from the image the
    /// checkpoint's flush kept, or read back from the device after recovery.
    /// </summary>
    [TestFixture]
    internal class InlineFlushBoundarySectorRecoveryTests : TestBase
    {
        /// <summary>The operations that supersede an in-memory record below ReadOnlyAddress in place. An Upsert does not: it inserts there
        /// without finding the record.</summary>
        public enum SupersedeOp { RMW, Delete }

        const long Key = 1;
        const long OriginalValue = 100;
        const long OtherKey = 2;

        /// <summary>Keys this far apart hash alike, so the later key's record heads the hash chain above the earlier key's.</summary>
        const long CollidingKeyDistance = 1000;

        /// <summary>The expiration every Upsert in this fixture writes; Tsavorite does not interpret it.</summary>
        const long Expiration = 1L << 40;

        /// <summary>Writes <see cref="Expiration"/> on every Upsert, and keeps the expiration of the record a Read found.</summary>
        sealed class ExpiringFunctions : SimpleLongSimpleFunctions
        {
            internal long LastExpiration;

            public override RecordFieldInfo GetUpsertFieldInfo<TKey>(TKey key, ReadOnlySpan<byte> value, ref long input)
            {
                var fieldInfo = base.GetUpsertFieldInfo(key, value, ref input);
                fieldInfo.HasExpiration = true;
                return fieldInfo;
            }

            public override bool InitialWriter(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref long input, ReadOnlySpan<byte> srcValue, ref long output, ref UpsertInfo upsertInfo)
                => base.InitialWriter(ref dstLogRecord, in sizeInfo, ref input, srcValue, ref output, ref upsertInfo) && dstLogRecord.TrySetExpiration(Expiration);

            public override bool Reader<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref long input, ref long output, ref ReadInfo readInfo)
            {
                LastExpiration = srcLogRecord.Expiration;
                return base.Reader(in srcLogRecord, ref input, ref output, ref readInfo);
            }
        }

        [SetUp]
        public void Setup() => DeleteDirectory(MethodTestDir, wait: true);

        [TearDown]
        public void TearDown() => OnTearDown();

        static TsavoriteKV<InlineStoreFunctions, InlineAllocator> CreateStore(IDevice log)
            => new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                MutableFraction = 0.9,
                LogMemorySize = 1L << 16,
                PageSize = 1L << 13,
                CheckpointDir = MethodTestDir
            }, StoreFunctions.Create(new LongKeyComparerModulo(CollidingKeyDistance), SpanByteRecordTriggers.Instance)
             , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));

        static ClientSession<TestSpanByteKey, long, long, Empty, ExpiringFunctions, InlineStoreFunctions, InlineAllocator> NewSession(
                TsavoriteKV<InlineStoreFunctions, InlineAllocator> store, ExpiringFunctions functions = null)
            => store.NewSession<TestSpanByteKey, long, long, Empty, ExpiringFunctions>(functions ?? new ExpiringFunctions());

        static byte[] Bytes(long value)
        {
            var bytes = new byte[sizeof(long)];
            new Span<byte>(bytes).AsRef<long>() = value;
            return bytes;
        }

        static void Upsert(TsavoriteKV<InlineStoreFunctions, InlineAllocator> store, long key, long value)
        {
            using var session = NewSession(store);
            _ = session.BasicContext.Upsert(TestSpanByteKey.FromArray(Bytes(key)), Bytes(value));
        }

        static void Supersede(TsavoriteKV<InlineStoreFunctions, InlineAllocator> store, SupersedeOp op, long key)
        {
            using var session = NewSession(store);
            var context = session.BasicContext;
            switch (op)
            {
                case SupersedeOp.RMW:
                    long input = 99, output = 0;
                    if (context.RMW(TestSpanByteKey.FromArray(Bytes(key)), ref input, ref output).IsPending)
                        _ = context.CompletePending(wait: true);
                    break;
                case SupersedeOp.Delete:
                    _ = context.Delete(TestSpanByteKey.FromArray(Bytes(key)));
                    break;
            }
        }

        static bool TryRead(TsavoriteKV<InlineStoreFunctions, InlineAllocator> store, long key, out long value, out long expiration)
        {
            var functions = new ExpiringFunctions();
            using var session = NewSession(store, functions);
            var context = session.BasicContext;
            long output = 0;
            var status = context.Read(TestSpanByteKey.FromArray(Bytes(key)), ref output);
            if (status.IsPending)
            {
                Assert.That(context.CompletePendingWithOutputs(out var completedOutputs, wait: true), Is.True);
                (status, output) = GetSinglePendingResult(completedOutputs);
            }
            value = output;
            expiration = functions.LastExpiration;
            return status.Found;
        }

        static void AssertOriginalRecord(TsavoriteKV<InlineStoreFunctions, InlineAllocator> store, string what)
        {
            Assert.That(TryRead(store, Key, out var value, out var expiration), Is.True, $"{what}: the FoldOver checkpoint lost key {Key}");
            Assert.That(value, Is.EqualTo(OriginalValue), $"{what}: the FoldOver checkpoint's value of key {Key} changed");
            Assert.That(expiration, Is.EqualTo(Expiration), $"{what}: the FoldOver checkpoint's expiration of key {Key} changed");
        }

        /// <summary>Asserts that the store's flushes end mid-sector, with <paramref name="recordAddress"/> in the sector the next flush writes again.</summary>
        static void AssertInBoundarySector(TsavoriteKV<InlineStoreFunctions, InlineAllocator> store, long recordAddress, int sectorSize)
        {
            var flushedUntil = store.hlogBase.FlushedUntilAddress;
            Assert.That(flushedUntil % sectorSize, Is.Not.Zero, "the checkpoint must end mid-sector");
            Assert.That(recordAddress, Is.GreaterThanOrEqualTo(RoundDown(flushedUntil, sectorSize)), "the record must sit in the sector the next flush writes again");
        }

        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task SupersedingABoundarySectorRecordKeepsTheFoldOverImage([Values] SupersedeOp op, [Values] bool flushInFlight)
        {
            using var log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "Boundary.log"), deleteOnClose: false);
            var gatedLog = new GatedCompletionDevice(log);
            var sectorSize = (int)log.SectorSize;

            var writeStore = CreateStore(gatedLog);
            var released = 0;
            try
            {
                var recordAddress = writeStore.Log.TailAddress;
                Upsert(writeStore, Key, OriginalValue);

                Assert.That(writeStore.TryInitiateFullCheckpoint(out _, CheckpointType.FoldOver), Is.True);
                await writeStore.CompleteCheckpointAsync().ConfigureAwait(false);
                AssertInBoundarySector(writeStore, recordAddress, sectorSize);

                var heldWrites = 0;
                if (flushInFlight)
                {
                    // Hold the next flush's device writes while the record is superseded. That flush writes the boundary sector on its own,
                    // followed by the rest of its span if the span reaches past that sector.
                    var flushedUntil = writeStore.hlogBase.FlushedUntilAddress;
                    Upsert(writeStore, OtherKey, 7);
                    heldWrites = RoundUp(writeStore.Log.TailAddress, sectorSize) - RoundDown(flushedUntil, sectorSize) > sectorSize ? 2 : 1;
                    gatedLog.Gate = true;
                    writeStore.hlogBase.ShiftReadOnlyAddressWithWait(writeStore.Log.TailAddress, wait: false);
                    gatedLog.WaitForPending(heldWrites, TimeSpan.FromSeconds(30));
                }

                Supersede(writeStore, op, Key);

                gatedLog.Gate = false;
                for (; released < heldWrites; released++)
                {
                    gatedLog.Release(released);
                    gatedLog.WaitForCompletion(released, TimeSpan.FromSeconds(30));
                }

                // The next flush starts in the boundary sector and writes it again, and so does the one after it, from the image the first
                // one kept of it.
                writeStore.Log.ShiftReadOnlyAddress(writeStore.Log.TailAddress, wait: true);
                Upsert(writeStore, OtherKey + 1, 8);
                writeStore.Log.ShiftReadOnlyAddress(writeStore.Log.TailAddress, wait: true);
            }
            finally
            {
                // Never leave a write captured, or the store's disposal waits for it.
                gatedLog.Gate = false;
                for (; released < gatedLog.CapturedCount; released++)
                    gatedLog.Release(released);
                writeStore.Dispose();
            }

            var recoverStore = CreateStore(log);
            try
            {
                _ = await recoverStore.RecoverAsync().ConfigureAwait(false);
                AssertOriginalRecord(recoverStore, $"{op}");
            }
            finally
            {
                recoverStore.Dispose();
            }
        }

        /// <summary>
        /// A Delete that cannot elide its source clears the source's optional fields in the live page. The next flush must not write that to
        /// the checkpoint's image of the record.
        /// </summary>
        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task DeletingABoundarySectorRecordKeepsItsExpiration()
        {
            using var log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "Expiration.log"), deleteOnClose: false);
            var sectorSize = (int)log.SectorSize;

            var writeStore = CreateStore(log);
            try
            {
                var recordAddress = writeStore.Log.TailAddress;
                Upsert(writeStore, Key, OriginalValue);

                // A record of a key that hashes alike heads the chain above the record, so the Delete below cannot elide it.
                Upsert(writeStore, Key + CollidingKeyDistance, 7);

                Assert.That(writeStore.TryInitiateFullCheckpoint(out _, CheckpointType.FoldOver), Is.True);
                await writeStore.CompleteCheckpointAsync().ConfigureAwait(false);
                AssertInBoundarySector(writeStore, recordAddress, sectorSize);

                Supersede(writeStore, SupersedeOp.Delete, Key);
                writeStore.Log.ShiftReadOnlyAddress(writeStore.Log.TailAddress, wait: true);
            }
            finally
            {
                writeStore.Dispose();
            }

            var recoverStore = CreateStore(log);
            try
            {
                _ = await recoverStore.RecoverAsync().ConfigureAwait(false);
                AssertOriginalRecord(recoverStore, "Delete");
            }
            finally
            {
                recoverStore.Dispose();
            }
        }

        /// <summary>
        /// Recovery restores FlushedUntilAddress at the FoldOver's tail, mid-sector, so the first flush after recovery writes the boundary
        /// sector again, with no image of it. It must read the sector back rather than take it from the live page, or recovering the same
        /// checkpoint again loses what the recovered store superseded there.
        /// </summary>
        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task SupersedingARecoveredBoundarySectorRecordKeepsTheFoldOverImage([Values] SupersedeOp op)
        {
            using var log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "Recovered.log"), deleteOnClose: false);
            var sectorSize = (int)log.SectorSize;
            Guid token;
            long recordAddress;

            var store = CreateStore(log);
            try
            {
                recordAddress = store.Log.TailAddress;
                Upsert(store, Key, OriginalValue);
                Assert.That(store.TryInitiateFullCheckpoint(out token, CheckpointType.FoldOver), Is.True);
                await store.CompleteCheckpointAsync().ConfigureAwait(false);
            }
            finally
            {
                store.Dispose();
            }

            // Supersede the record in the recovered store and flush, without taking another checkpoint.
            store = CreateStore(log);
            try
            {
                _ = await store.RecoverAsync(default, token).ConfigureAwait(false);
                AssertInBoundarySector(store, recordAddress, sectorSize);
                Supersede(store, op, Key);
                store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
            }
            finally
            {
                store.Dispose();
            }

            store = CreateStore(log);
            try
            {
                _ = await store.RecoverAsync(default, token).ConfigureAwait(false);
                AssertOriginalRecord(store, $"{op} after recovery");
            }
            finally
            {
                store.Dispose();
            }
        }

        /// <summary>
        /// When BeginAddress passes ReadOnlyAddress, the flush of the range below it is skipped, so nothing was written to the sector the next
        /// flush starts in. Nothing reads the bytes below BeginAddress, so that flush takes them from the live page rather than reading back
        /// bytes that were never written.
        /// </summary>
        [Test]
        [Category("TsavoriteKV")]
        public void AFlushAfterASkippedRangeStartsMidSector()
        {
            using var log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "Skipped.log"), deleteOnClose: true);
            var sectorSize = (int)log.SectorSize;

            var store = CreateStore(log);
            try
            {
                const int keyCount = 20;
                var addresses = new long[keyCount + 1];
                for (var key = 1; key <= keyCount; key++)
                {
                    addresses[key] = store.Log.TailAddress;
                    Upsert(store, key, key);
                }

                var firstKept = keyCount / 2;
                while (addresses[firstKept] % sectorSize == 0)
                    firstKept++;
                var newBeginAddress = addresses[firstKept];
                Assert.That(newBeginAddress, Is.GreaterThan(store.hlogBase.ReadOnlyAddress), "BeginAddress must pass ReadOnlyAddress");

                store.Log.ShiftBeginAddress(newBeginAddress);
                Assert.That(store.hlogBase.FlushedUntilAddress, Is.EqualTo(newBeginAddress), "the range below BeginAddress should be skipped, not written");

                store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
                Assert.That(store.hlogBase.FlushedUntilAddress, Is.EqualTo(store.Log.TailAddress), "the flush after the skipped range did not complete");

                store.Log.FlushAndEvict(wait: true);
                for (var key = firstKept; key <= keyCount; key++)
                {
                    Assert.That(TryRead(store, key, out var value, out _), Is.True, $"key {key} is not on disk");
                    Assert.That(value, Is.EqualTo(key), $"key {key} is not intact on disk");
                }
            }
            finally
            {
                store.Dispose();
            }
        }
    }
}