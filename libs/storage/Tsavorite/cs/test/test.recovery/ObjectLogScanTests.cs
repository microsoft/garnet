// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test
{
    // Must be in a separate block so the "using ClassStoreFunctions" is the first line in its namespace declaration.
    public class TestObjectValueComparerModulo : IKeyComparer
    {
        readonly long mod;

        internal TestObjectValueComparerModulo(long mod) => this.mod = mod;

        public bool Equals<TFirstKey, TSecondKey>(TFirstKey k1, TSecondKey k2)
            where TFirstKey : IKey, allows ref struct
            where TSecondKey : IKey, allows ref struct
            => k1.KeyBytes.AsRef<TestObjectKey>().key == k2.KeyBytes.AsRef<TestObjectKey>().key;

        // Force collisions to create a chain
        public long GetHashCode64<TKey>(TKey key)
            where TKey : IKey, allows ref struct
        {
            long hash = Utility.GetHashCode(key.KeyBytes.AsRef<TestObjectKey>().key);
            return mod > 0 ? hash % mod : hash;
        }
    }
}

namespace Tsavorite.test
{
    using ClassAllocator = ObjectAllocator<StoreFunctions<TestObjectValueComparerModulo, DefaultRecordTriggers>>;
    using ClassStoreFunctions = StoreFunctions<TestObjectValueComparerModulo, DefaultRecordTriggers>;
    using DisposingAllocator = ObjectAllocator<StoreFunctions<TestObjectValueComparerModulo, ObjectLogScanTests.DisposingScanTriggers>>;
    using DisposingStoreFunctions = StoreFunctions<TestObjectValueComparerModulo, ObjectLogScanTests.DisposingScanTriggers>;
    [TestFixture]
    internal class ObjectLogScanTests : TestBase
    {
        private TsavoriteKV<ClassStoreFunctions, ClassAllocator> store;
        private IDevice log, objlog;
        const int TotalRecords = 250;
        const int OverflowKeyLength = 2048;

        TestObjectValueComparerModulo comparer;

        readonly struct OverflowTestKey : IKey
        {
            // Large enough to force the key into the object allocator's overflow storage.
            readonly byte[] bytes;

            internal OverflowTestKey(int key, int length = OverflowKeyLength)
            {
                bytes = new byte[length];
                _ = BitConverter.TryWriteBytes(bytes, key);
            }

            public bool IsPinned => false;
            public bool IsEmpty => false;
            public ReadOnlySpan<byte> KeyBytes => bytes;
            public bool HasNamespace => false;
            public ReadOnlySpan<byte> NamespaceBytes => [];
        }

        sealed class TrackingHeapObject(bool blockOnClear) : IHeapObject
        {
            // Signals that cleanup captured this object and entered its callback.
            internal readonly ManualResetEventSlim clearEntered = new(false);

            // Keeps cleanup blocked while the test evicts the source record and its object-map page.
            internal readonly ManualResetEventSlim releaseClear = new(!blockOnClear);

            // Models whether checkpoint serialization is currently cached.
            int cachedDataPresent = 1;

            // Counts successful cached-data clears.
            int clearCount;

            internal int ClearCount => Volatile.Read(ref clearCount);

            public long HeapMemorySize => 0;
            public IHeapObject Clone() => new TrackingHeapObject(blockOnClear: false);
            public void Dispose() { }
            public void DoSerialize(BinaryWriter writer) => writer.Write(0);
            public void Serialize(BinaryWriter writer)
            {
                WriteType(writer, isNull: false);
                DoSerialize(writer);
            }
            public void WriteType(BinaryWriter writer, bool isNull) => writer.Write(isNull);
            public bool CacheSerializedObjectData(ref LogRecord dstLogRecord, bool srcIsOnMemoryLog, bool checkpointNeedsSourceImage)
            {
                if (!checkpointNeedsSourceImage)
                    return false;
                Volatile.Write(ref cachedDataPresent, 1);
                return true;
            }
            public void ClearSerializedObjectData()
            {
                if (Interlocked.Exchange(ref cachedDataPresent, 0) == 0)
                    return;

                clearEntered.Set();
                releaseClear.Wait();
                _ = Interlocked.Increment(ref clearCount);
            }
        }

        sealed class TrackingHeapObjectSerializer : BinaryObjectSerializer<IHeapObject>
        {
            public override void Deserialize(out IHeapObject obj) => obj = new TrackingHeapObject(blockOnClear: false);
            public override void Serialize(IHeapObject obj) => writer.Write(0);
        }

        sealed class BlockingSerializationHeapObject : HeapObjectBase
        {
            // Coordinates cleanup with a direct serialization paused in DoSerialize.
            internal readonly ManualResetEventSlim serializeEntered = new(false);
            internal readonly ManualResetEventSlim releaseSerialize = new(false);

            public override IHeapObject Clone() => new BlockingSerializationHeapObject();
            public override void Dispose() { }
            public override void DoSerialize(BinaryWriter writer)
            {
                serializeEntered.Set();
                releaseSerialize.Wait();
                writer.Write(0);
            }
            public override void WriteType(BinaryWriter writer, bool isNull) => writer.Write(isNull);
        }

        sealed class CountingSerializationHeapObject : HeapObjectBase
        {
            internal const string SerializeFailureMessage = "Injected DoSerialize failure";

            // Counts entries into the direct-serialize path, and records which indicator Serialize() wrote.
            internal int doSerializeCount;
            internal bool? lastWriteTypeIsNull;
            internal bool throwOnSerialize;

            public override IHeapObject Clone() => new CountingSerializationHeapObject();
            public override void Dispose() { }
            public override void DoSerialize(BinaryWriter writer)
            {
                _ = Interlocked.Increment(ref doSerializeCount);
                if (throwOnSerialize)
                    throw new InvalidOperationException(SerializeFailureMessage);
                writer.Write(0);
            }
            public override void WriteType(BinaryWriter writer, bool isNull)
            {
                lastWriteTypeIsNull = isNull;
                writer.Write(isNull);
            }
        }

        [SetUp]
        public void Setup()
        {
            // Clean up log files from previous test runs in case they weren't cleaned up
            DeleteDirectory(MethodTestDir, wait: true);

            comparer = null;
            foreach (var arg in TestContext.CurrentContext.Test.Arguments)
            {
                if (arg is HashModulo mod && mod == HashModulo.Hundred)
                {
                    comparer = new TestObjectValueComparerModulo(100);
                    continue;
                }
            }
            comparer ??= new(0);
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

        /// <summary>Writes no bytes at all, so the record's value component is zero length on the object log.</summary>
        sealed class ZeroLengthValueSerializer : BinaryObjectSerializer<IHeapObject>
        {
            public override void Deserialize(out IHeapObject obj) => obj = new TrackingHeapObject(blockOnClear: false);
            public override void Serialize(IHeapObject obj) { }
        }

        /// <summary>
        /// A zero-length value component survives the page read path (scan of an evicted page).
        /// </summary>
        /// <remarks>
        /// NOTE: this does NOT yet pin the all-zero-length-page guard in <c>DeserializeObjectsOnPage</c>. This fixture's keys are
        /// <c>OverflowTestKey</c>, so the page walk's extent is dominated by the overflow KEYS -- measured at 10,264 bytes for the
        /// four records here -- and the walk never sees the zero total that trips "TotalLength cannot be 0". Verified by negative
        /// control: forcing <c>hasBytesToRead = true</c> leaves this test passing.
        /// <para>To pin that guard, the only out-of-line component on the page must be the zero-length value, which needs an
        /// INLINE key; see <c>ComputeObjectIdValueSizeHint</c>, where a component of 511 bytes or fewer is stamped exact-size with
        /// its byte count as the hint, so a zero-length one decodes to a zero extent while still being stamped and still counted
        /// into <c>startPosition</c>. The other branch cannot reach zero: a component over 511 bytes is stamped with a 4 KB page
        /// count, which is at least 1.</para>
        /// </remarks>
        [Test]
        [Category("TsavoriteKV")]
        public void ZeroLengthValueComponentSurvivesThePageReadPath()
        {
            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "ZeroLenPage.log"), deleteOnClose: true);
            objlog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "ZeroLenPage.obj.log"), deleteOnClose: true);
            store = new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                ObjectLogDevice = objlog,
                MutableFraction = 0.1,
                LogMemorySize = 1L << 15,
                PageSize = MinKvLogPageSize
            }, StoreFunctions.Create(comparer, () => new ZeroLengthValueSerializer())
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            const int numRecords = 4;
            using (var session = store.NewSession<OverflowTestKey, TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions()))
            {
                var context = session.BasicContext;
                for (var ii = 0; ii < numRecords; ++ii)
                    _ = context.Upsert(new OverflowTestKey(ii), new TrackingHeapObject(blockOnClear: false), Empty.Default);
            }

            // Push every record below HeadAddress so the scan must read the page back from the device rather than walk memory.
            store.Log.FlushAndEvict(wait: true);
            Assert.That(store.Log.HeadAddress, Is.EqualTo(store.Log.TailAddress), "records must be evicted for the page path to run");

            var scanned = 0;
            using (var iter = store.Log.Scan(store.Log.BeginAddress, store.Log.TailAddress))
            {
                while (iter.GetNext())
                    ++scanned;
            }

            Assert.That(scanned, Is.EqualTo(numRecords), "every zero-length-component record must still be returned by the scan");
        }

        /// <summary>
        /// A pending read that walks past records with other overflow keys in its hash chain finds the older key, and frees only the transient
        /// slots it allocated. A mismatched record's value field still holds its flushed slot id, which is not a transient slot.
        /// </summary>
        [Test]
        [Category("TsavoriteKV")]
        public void PendingReadPastOtherOverflowKeysFreesOnlyItsOwnSlots()
        {
            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "OverflowKeyChain.log"), deleteOnClose: true);
            objlog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "OverflowKeyChain.obj.log"), deleteOnClose: true);

            // A modulo of 1 hashes every key to 0, so all records share one hash chain.
            store = new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                ObjectLogDevice = objlog,
                MutableFraction = 0.1,
                LogMemorySize = 1L << 15,
                PageSize = MinKvLogPageSize
            }, StoreFunctions.Create(new TestObjectValueComparerModulo(1), () => new TestObjectValue.Serializer())
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            const int numRecords = 3;
            using var session = store.NewSession<OverflowTestKey, TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions());
            var context = session.BasicContext;
            for (var ii = 0; ii < numRecords; ++ii)
                _ = context.Upsert(new OverflowTestKey(ii), new TestObjectValue { value = ii }, Empty.Default);
            store.Log.FlushAndEvict(wait: true);

            // Key 0 is the oldest, so its read walks past the record of every other key.
            TestObjectInput input = new();
            TestObjectOutput output = new();
            var status = context.Read(new OverflowTestKey(0), ref input, ref output, Empty.Default);
            if (status.IsPending)
            {
                Assert.That(context.CompletePendingWithOutputs(out var outputs, wait: true), Is.True);
                (status, output) = GetSinglePendingResult(outputs);
            }
            Assert.That(status.Found, Is.True, "The read did not find the oldest key past the others");
            Assert.That(output.value.value, Is.EqualTo(0));

            // Every pending read has completed, so each slot the transient map has handed out is free again, exactly once.
            var transientMap = store.hlogBase._wrapper.TransientObjectIdMap;
            List<int> freeSlots = [];
            while (transientMap.freeSlots.TryPop(out var slot))
                freeSlots.Add(slot);
            Assert.That(freeSlots, Has.All.LessThan(transientMap.Count), "A read freed a slot the transient map never handed out");
            Assert.That(freeSlots, Is.Unique, "A read freed a transient slot twice");
        }

        [Test]
        [Category("TsavoriteKV")]
        public async Task SerializedObjectCleanupTest()
        {
            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "SerializedObjectCleanup.log"), deleteOnClose: true);
            objlog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "SerializedObjectCleanup.obj.log"), deleteOnClose: true);
            store = new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                ObjectLogDevice = objlog,
                MutableFraction = 0.1,
                LogMemorySize = 1L << 15,
                PageSize = MinKvLogPageSize
            }, StoreFunctions.Create(comparer, () => new TrackingHeapObjectSerializer())
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            using var session = store.NewSession<OverflowTestKey, TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions());
            var context = session.BasicContext;
            var value = new TrackingHeapObject(blockOnClear: true);
            var valuePastEnd = new TrackingHeapObject(blockOnClear: false);
            try
            {
                // Include the first overflow-key record in the cleanup range and exclude the second.
                var beginAddress = store.Log.TailAddress;
                _ = context.Upsert(new OverflowTestKey(1), value, Empty.Default);
                var endAddress = store.Log.TailAddress;
                _ = context.Upsert(new OverflowTestKey(2), valuePastEnd, Empty.Default);

                // Confirm this test exercises the key representation that the generic scan failed to remap.
                var record = store.hlogBase._wrapper.CreateLogRecord(beginAddress);
                Assert.That(record.DataHeader.KeyIsOverflow, Is.True);

                // TrackingHeapObject fabricates cached state (cachedDataPresent starts at 1) rather than going through
                // CacheSerializedObjectData, so it must also announce the cache that gates and bounds the sweep.
                store.hlogBase.NoteSerializedObjectCache(beginAddress);

                // Pause after cleanup captures the managed value reference.
                var cleanupTask = Task.Run(() => store.Log.ClearSerializedObjectData(beginAddress, endAddress));
                Assert.That(value.clearEntered.Wait(TimeSpan.FromSeconds(5)), Is.True, "Cleanup did not capture the heap value");

                // Eviction must complete while cleanup is blocked, proving the callback does not retain epoch protection.
                var evictionTask = Task.Run(() => store.Log.FlushAndEvict(wait: true));
                var completedTask = await Task.WhenAny(evictionTask, Task.Delay(TimeSpan.FromSeconds(5))).ConfigureAwait(false);
                Assert.That(completedTask, Is.SameAs(evictionTask), "Cleanup retained epoch protection while clearing the heap value");
                await evictionTask.ConfigureAwait(false);

                // The captured reference remains callable after eviction clears the record's object-map slot.
                value.releaseClear.Set();
                await cleanupTask.ConfigureAwait(false);

                // Cleanup affects the included record exactly once and does not cross the exclusive end address.
                Assert.That(value.ClearCount, Is.EqualTo(1));
                Assert.That(valuePastEnd.ClearCount, Is.Zero);
            }
            finally
            {
                value.releaseClear.Set();
            }
        }

        [Test]
        [Category("TsavoriteKV")]
        public void SweepIsSkippedWhenNothingWasCached()
        {
            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "SweepSkip.log"), deleteOnClose: true);
            objlog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "SweepSkip.obj.log"), deleteOnClose: true);
            store = new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                ObjectLogDevice = objlog,
                MutableFraction = 0.1,
                LogMemorySize = 1L << 15,
                PageSize = MinKvLogPageSize
            }, StoreFunctions.Create(comparer, () => new TrackingHeapObjectSerializer())
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            using var session = store.NewSession<OverflowTestKey, TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions());
            var context = session.BasicContext;
            var value = new TrackingHeapObject(blockOnClear: false);

            var beginAddress = store.Log.TailAddress;
            _ = context.Upsert(new OverflowTestKey(1), value, Empty.Default);
            var endAddress = store.Log.TailAddress;

            // No CopyUpdate has cached a (v) image, so the sweep must not walk the range at all.
            store.Log.ClearSerializedObjectData(beginAddress, endAddress);
            Assert.That(value.ClearCount, Is.Zero, "the sweep walked the range when nothing had been cached");

            // Negative control: the skip is a gate, not a permanent disable. Announcing a cache makes the very same
            // sweep do its work, which proves the range and the record were reachable all along.
            store.hlogBase.NoteSerializedObjectCache(beginAddress);
            store.Log.ClearSerializedObjectData(beginAddress, endAddress);
            Assert.That(value.ClearCount, Is.EqualTo(1), "the sweep did not run after a cache was announced");

            // The gate re-arms: with that cache consumed, a further sweep is skipped again.
            store.Log.ClearSerializedObjectData(beginAddress, endAddress);
            Assert.That(value.ClearCount, Is.EqualTo(1), "the sweep ran again with no new cache");
        }

        [Test]
        [Category("TsavoriteKV")]
        public void SweepIsNarrowedToTheCachedAddressRange()
        {
            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "SweepNarrow.log"), deleteOnClose: true);
            objlog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "SweepNarrow.obj.log"), deleteOnClose: true);
            store = new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                ObjectLogDevice = objlog,
                MutableFraction = 0.1,
                LogMemorySize = 1L << 15,
                PageSize = MinKvLogPageSize
            }, StoreFunctions.Create(comparer, () => new TrackingHeapObjectSerializer())
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            using var session = store.NewSession<OverflowTestKey, TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions());
            var context = session.BasicContext;

            var below = new TrackingHeapObject(blockOnClear: false);
            var cached = new TrackingHeapObject(blockOnClear: false);
            var above = new TrackingHeapObject(blockOnClear: false);

            var beginAddress = store.Log.TailAddress;
            _ = context.Upsert(new OverflowTestKey(1), below, Empty.Default);
            var cachedAddress = store.Log.TailAddress;
            _ = context.Upsert(new OverflowTestKey(2), cached, Empty.Default);
            _ = context.Upsert(new OverflowTestKey(3), above, Empty.Default);
            var endAddress = store.Log.TailAddress;

            // Only the middle record's (v) image was cached, so only it is in the recorded extent.
            store.hlogBase.NoteSerializedObjectCache(cachedAddress);
            store.Log.ClearSerializedObjectData(beginAddress, endAddress);

            Assert.That(cached.ClearCount, Is.EqualTo(1), "the cached record was not cleared");
            Assert.That(below.ClearCount, Is.Zero, "the walk was not clamped at its start to the cached extent");
            Assert.That(above.ClearCount, Is.Zero, "the walk was not clamped at its end to the cached extent");

            // Negative control: the two outer records are reachable from the same caller range, and are cleared once the
            // recorded extent spans them. Without this, the assertions above would also pass if the sweep did nothing.
            store.hlogBase.NoteSerializedObjectCache(beginAddress);
            store.hlogBase.NoteSerializedObjectCache(endAddress - 1);
            store.Log.ClearSerializedObjectData(beginAddress, endAddress);

            Assert.That(below.ClearCount, Is.EqualTo(1), "the record below the previous extent was never reachable");
            Assert.That(above.ClearCount, Is.EqualTo(1), "the record above the previous extent was never reachable");
        }

        [Test]
        [Category("TsavoriteKV")]
        public void CachingIsSkippedWhenTheCheckpointDoesNotNeedTheSourceImage()
        {
            var value = new CountingSerializationHeapObject();
            _ = CreateStoreWithCachedSerializationSource("NoSourceImageNeeded", value, out var recordAddress);

            var logRecord = store.hlogBase._wrapper.CreateLogRecord(recordAddress);

            // The checkpoint has no (v) image at stake for this source, so the bytes must not be serialized, and the object must be
            // superseded: the CopyUpdate releases the source's value, and a flush still walking the source writes a null value for it.
            var cached = value.CacheSerializedObjectData(ref logRecord, srcIsOnMemoryLog: true, checkpointNeedsSourceImage: false);

            Assert.That(cached, Is.False, "the object reported caching bytes it was told were not needed");
            Assert.That(value.doSerializeCount, Is.Zero, "the (v) image was serialized despite not being needed");
            using (var stream = new MemoryStream())
            using (var writer = new BinaryWriter(stream))
                value.Serialize(writer);
            Assert.That(value.doSerializeCount, Is.Zero, "a superseded object was serialized from its live state");
            Assert.That(value.lastWriteTypeIsNull, Is.True, "a superseded object with no cached bytes must write the null indicator");

            // Negative control: an object in the same starting state, told the image IS needed, does serialize. This
            // proves the path is reached and the assertions above are not passing for an unrelated reason. It must be
            // a fresh object: the skip path above deliberately leaves the phase terminal, so re-running on `value`
            // would short-circuit for that reason instead.
            var control = new CountingSerializationHeapObject();
            var cachedWhenNeeded = control.CacheSerializedObjectData(ref logRecord, srcIsOnMemoryLog: true, checkpointNeedsSourceImage: true);
            Assert.That(cachedWhenNeeded, Is.True);
            Assert.That(control.doSerializeCount, Is.EqualTo(1));
        }

        [Test]
        [Category("TsavoriteKV")]
        public async Task CleanupWithoutCachedDataTest()
        {
            var value = new BlockingSerializationHeapObject();

            // Direct serialization enters SERIALIZING without creating cached checkpoint bytes.
            var serializationTask = Task.Run(() =>
            {
                using var stream = new MemoryStream();
                using var writer = new BinaryWriter(stream);
                value.Serialize(writer);
            });

            try
            {
                Assert.That(value.serializeEntered.Wait(TimeSpan.FromSeconds(5)), Is.True);

                // With no cached bytes, cleanup must leave the active serialization phase unchanged.
                value.ClearSerializedObjectData();

                // This transition can only succeed if cleanup incorrectly reset the phase to REST.
                Assert.That(value.MakeTransition(SerializationPhase.REST, SerializationPhase.SERIALIZED), Is.False,
                    "Cleanup reset an object that had no cached serialization");
            }
            finally
            {
                value.releaseSerialize.Set();
            }

            await serializationTask.ConfigureAwait(false);
        }

        /// <summary>
        /// Creates the store used by the cached-serialization tests, upserts <paramref name="value"/>, and marks its
        /// record as the new version, which is what makes CacheSerializedObjectData capture the (v) bytes rather
        /// than simply hand the object off. Returns the address range covering that single record.
        /// </summary>
        private (long beginAddress, long endAddress) CreateStoreWithCachedSerializationSource(string name, IHeapObject value, out long recordAddress)
        {
            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, $"{name}.log"), deleteOnClose: true);
            objlog = Devices.CreateLogDevice(Path.Join(MethodTestDir, $"{name}.obj.log"), deleteOnClose: true);
            store = new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                ObjectLogDevice = objlog,
                MutableFraction = 0.1,
                LogMemorySize = 1L << 15,
                PageSize = MinKvLogPageSize
            }, StoreFunctions.Create(comparer, () => new TrackingHeapObjectSerializer())
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            using var session = store.NewSession<OverflowTestKey, TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions());
            var context = session.BasicContext;

            var beginAddress = store.Log.TailAddress;
            _ = context.Upsert(new OverflowTestKey(1), value, Empty.Default);
            var endAddress = store.Log.TailAddress;

            recordAddress = beginAddress;
            var logRecord = store.hlogBase._wrapper.CreateLogRecord(beginAddress);
            logRecord.InfoRef.SetIsInNewVersion();
            return (beginAddress, endAddress);
        }

        /// <summary>
        /// Regression test for #2101. A CopyUpdate during a checkpoint caches the superseded (v) object's bytes
        /// and leaves it SERIALIZED; post-checkpoint cleanup then releases those bytes. Because Clone() is a
        /// shallow copy, the (v+1) record that superseded it shares and keeps mutating its collections, so the
        /// object must stay terminal. Returning it to REST let the next checkpoint take the direct-serialize
        /// path and enumerate those live collections, throwing "Collection was modified" out of DoSerialize and
        /// wedging the checkpoint pipeline.
        /// </summary>
        [Test]
        [Category("TsavoriteKV")]
        public void CleanupOfCachedDataLeavesObjectTerminal()
        {
            var value = new CountingSerializationHeapObject();
            var (beginAddress, endAddress) = CreateStoreWithCachedSerializationSource("CachedDataCleanup", value, out var recordAddress);

            // Drive the CopyUpdate-during-checkpoint path that caches the (v) bytes.
            var logRecord = store.hlogBase._wrapper.CreateLogRecord(recordAddress);
            value.CacheSerializedObjectData(ref logRecord, srcIsOnMemoryLog: true, checkpointNeedsSourceImage: true);

            // The sweep is gated on a cache having been noted, and bounded by the address noted with it; InternalRMW does
            // that in production, so a test that drives the object directly must do it too or the sweep below skips.
            store.hlogBase.NoteSerializedObjectCache(recordAddress);

            // Caching serializes once, into the cached byte[] rather than to a writer.
            Assert.That(value.doSerializeCount, Is.EqualTo(1), "CacheSerializedObjectData did not capture the (v) bytes");

            // Post-checkpoint cleanup releases the cached bytes.
            store.Log.ClearSerializedObjectData(beginAddress, endAddress);

            // This transition can only succeed if cleanup incorrectly reset the phase to REST.
            Assert.That(value.MakeTransition(SerializationPhase.REST, SerializationPhase.SERIALIZING), Is.False,
                "Cleanup returned a superseded object to REST");

            using var stream = new MemoryStream();
            using var writer = new BinaryWriter(stream);
            value.Serialize(writer);

            // SERIALIZED with no cached bytes means the object was superseded after the checkpoint completed:
            // the superseding record sits at a higher address and carries the live data, so this one writes null.
            Assert.That(value.doSerializeCount, Is.EqualTo(1), "A superseded object was re-serialized from its live state");
            Assert.That(value.lastWriteTypeIsNull, Is.True, "A superseded object with no cached bytes must write the null indicator");
        }

        /// <summary>
        /// A failing DoSerialize on the direct path (Serialize straight to the wire) must not strand the object
        /// in SERIALIZING: every later Serialize() and CacheSerializedObjectData() spins waiting for that phase
        /// to clear, so the checkpoint pipeline would hang rather than report the failure.
        /// </summary>
        [Test]
        [Category("TsavoriteKV")]
        public void FailedSerializationRestoresRestPhase()
        {
            var value = new CountingSerializationHeapObject { throwOnSerialize = true };

            using var stream = new MemoryStream();
            using var writer = new BinaryWriter(stream);

            var failure = Assert.Throws<InvalidOperationException>(() => value.Serialize(writer));
            Assert.That(failure.Message, Is.EqualTo(CountingSerializationHeapObject.SerializeFailureMessage));
            Assert.That(value.doSerializeCount, Is.EqualTo(1));

            // Only succeeds if the failed serialization restored REST.
            Assert.That(value.MakeTransition(SerializationPhase.REST, SerializationPhase.SERIALIZING), Is.True,
                "A failed serialization stranded the object outside REST");
        }

        /// <summary>
        /// The same guarantee for the other DoSerialize caller: the CopyUpdate-during-checkpoint path that captures
        /// the (v) bytes. A failure there must rethrow, restore REST, and publish no partial capture; stranding the
        /// object in SERIALIZING would hang every later serialization attempt on it.
        /// </summary>
        [Test]
        [Category("TsavoriteKV")]
        public void FailedCachedSerializationRestoresRestPhase()
        {
            var value = new CountingSerializationHeapObject { throwOnSerialize = true };
            _ = CreateStoreWithCachedSerializationSource("FailedCachedSerialization", value, out var recordAddress);

            var failure = Assert.Throws<InvalidOperationException>(() =>
            {
                // The ref local must be created inside the lambda; ref locals cannot be captured.
                var record = store.hlogBase._wrapper.CreateLogRecord(recordAddress);
                value.CacheSerializedObjectData(ref record, srcIsOnMemoryLog: true, checkpointNeedsSourceImage: true);
            });
            Assert.That(failure.Message, Is.EqualTo(CountingSerializationHeapObject.SerializeFailureMessage));
            Assert.That(value.doSerializeCount, Is.EqualTo(1));

            // Only succeeds if the failed capture restored REST rather than leaving SERIALIZING behind.
            Assert.That(value.MakeTransition(SerializationPhase.REST, SerializationPhase.SERIALIZING), Is.True,
                "A failed cached serialization stranded the object outside REST");
            Assert.That(value.MakeTransition(SerializationPhase.SERIALIZING, SerializationPhase.REST), Is.True);

            // No partial capture was published: a later Serialize re-serializes from the live object and writes it
            // as present, rather than emitting a truncated cached buffer or the superseded null indicator.
            value.throwOnSerialize = false;
            using var stream = new MemoryStream();
            using var writer = new BinaryWriter(stream);
            value.Serialize(writer);

            Assert.That(value.doSerializeCount, Is.EqualTo(2), "The object did not re-serialize from its live state");
            Assert.That(value.lastWriteTypeIsNull, Is.False, "A failed capture must not leave the object looking superseded");
        }

        internal struct ObjectPushScanTestFunctions : IScanIteratorFunctions
        {
            internal long numRecords;

            public readonly bool OnStart(long beginAddress, long endAddress) => true;

            public bool Reader<TSourceLogRecord>(in TSourceLogRecord logRecord, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                where TSourceLogRecord : ISourceLogRecord
            {
                cursorRecordResult = CursorRecordResult.Accept; // default; not used here
                ClassicAssert.AreEqual(numRecords, logRecord.Key.AsRef<TestObjectKey>().key, $"log scan 1: key");
                ClassicAssert.AreEqual(numRecords, ((TestObjectValue)logRecord.ValueObject).value, $"log scan 1: value");

                ++numRecords;
                return true;
            }

            public readonly void OnException(Exception exception, long numberOfRecords) { }

            public readonly void OnStop(bool completed, long numberOfRecords) { }
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void DiskWriteScanBasicTest([Values] TestDeviceType deviceType, [Values] ScanIteratorType scanIteratorType)
        {
            log = CreateTestDevice(deviceType, Path.Join(MethodTestDir, $"DiskWriteScanBasicTest_{deviceType}.log"));
            objlog = CreateTestDevice(deviceType, Path.Join(MethodTestDir, $"DiskWriteScanBasicTest_{deviceType}.obj.log"));
            store = new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                ObjectLogDevice = objlog,
                MutableFraction = 0.1,
                LogMemorySize = 1L << 15,
                PageSize = MinKvLogPageSize,
                SegmentSize = 1L << 18,
                ObjectLogSegmentSize = 1L << 22
            }, StoreFunctions.Create(comparer, () => new TestObjectValue.Serializer())
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            using var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions());
            var bContext = session.BasicContext;

            using var s = store.Log.Subscribe(new LogObserver());

            var start = store.Log.TailAddress;
            for (int i = 0; i < TotalRecords; i++)
            {
                var _key = new TestObjectKey { key = i };
                var _value = new TestObjectValue { value = i };
                _ = bContext.Upsert(_key, _value, Empty.Default);
                if (i % 100 == 0)
                    store.Log.FlushAndEvict(true);
            }

            // One pass to verify in-memory scan
            scanAndVerify(DiskScanBufferingMode.SinglePageBuffering);

            store.Log.FlushAndEvict(true);

            ObjectPushScanTestFunctions scanIteratorFunctions = new();

            void scanAndVerify(DiskScanBufferingMode sbm)
            {
                scanIteratorFunctions.numRecords = 0;

                if (scanIteratorType == ScanIteratorType.Pull)
                {
                    using var iter = store.Log.Scan(start, store.Log.TailAddress, sbm);
                    while (iter.GetNext())
                        _ = scanIteratorFunctions.Reader(in iter, default, default, out _);
                }
                else
                    ClassicAssert.IsTrue(store.Log.Scan(ref scanIteratorFunctions, start, store.Log.TailAddress, sbm), "Failed to complete push iteration");

                ClassicAssert.AreEqual(TotalRecords, scanIteratorFunctions.numRecords);
            }

            scanAndVerify(DiskScanBufferingMode.SinglePageBuffering);
            scanAndVerify(DiskScanBufferingMode.DoublePageBuffering);
        }

        class LogObserver : IObserver<ITsavoriteScanIterator>
        {
            int val = 0;

            public void OnCompleted()
            {
                ClassicAssert.AreEqual(val == TotalRecords, $"LogObserver.OnCompleted: totalRecords");
            }

            public void OnError(Exception error)
            {
            }

            public void OnNext(ITsavoriteScanIterator iter)
            {
                while (iter.GetNext())
                {
                    ClassicAssert.AreEqual(val, iter.Key.AsRef<TestObjectKey>().key, $"LogObserver.OnNext: key");
                    ClassicAssert.AreEqual(val, ((TestObjectValue)iter.ValueObject).value, $"LogObserver.OnNext: value");
                    val++;
                }
            }
        }

        /// <summary>
        /// Key whose length varies with the ordinal, so record sizes do not divide the device sector size evenly and some record
        /// therefore has a sector boundary running through it. The comparer reads only the leading <see cref="TestObjectKey"/>.
        /// </summary>
        readonly struct VariableLengthTestKey : IKey
        {
            readonly byte[] bytes;

            internal VariableLengthTestKey(int key)
            {
                bytes = new byte[sizeof(int) + (key % 8)];
                _ = BitConverter.TryWriteBytes(bytes, key);
            }

            public bool IsPinned => false;
            public bool IsEmpty => false;
            public ReadOnlySpan<byte> KeyBytes => bytes;
            public bool HasNamespace => false;
            public ReadOnlySpan<byte> NamespaceBytes => [];
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category(IteratorCategory)]
        public void ObjectScanEndInsideRecordTest()
        {
            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "ObjectScanEndInsideRecordTest.log"), deleteOnClose: true);
            objlog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "ObjectScanEndInsideRecordTest.obj.log"), deleteOnClose: true);

            // Records never span pages, so a sector boundary can run through a record only if a page holds several sectors.
            var pageSize = Math.Max(MinKvLogPageSize, 2L * Math.Max(log.SectorSize, objlog.SectorSize));
            store = new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                ObjectLogDevice = objlog,
                MutableFraction = 0.1,
                LogMemorySize = 8 * pageSize,
                PageSize = pageSize
            }, StoreFunctions.Create(comparer, () => new TestObjectValue.Serializer())
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            using var session = store.NewSession<VariableLengthTestKey, TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions());
            var bContext = session.BasicContext;

            var start = store.Log.TailAddress;
            for (var ii = 0; ii < TotalRecords; ++ii)
                _ = bContext.Upsert(new VariableLengthTestKey(ii), new TestObjectValue { value = ii }, Empty.Default);

            store.Log.FlushAndEvict(wait: true);

            // Find a record that a device-sector boundary runs through, before its trailing alignment padding, so that a frame load
            // stopping at that boundary cuts off the record's object-log position and lengths.
            var sectorSize = store.hlogBase.GetDeviceSectorSize();
            long victimAddress = 0, victimNextAddress = 0;
            var victimKey = 0;
            var victimValue = 0;
            using (var iter = store.Log.Scan(start, store.Log.TailAddress))
            {
                while (iter.GetNext())
                {
                    var sectorBoundary = (iter.CurrentAddress + 8 + sectorSize - 1) / sectorSize * sectorSize;
                    if (sectorBoundary + 8 <= iter.NextAddress)
                    {
                        victimAddress = iter.CurrentAddress;
                        victimNextAddress = iter.NextAddress;
                        victimKey = iter.Key.AsRef<TestObjectKey>().key;
                        victimValue = ((TestObjectValue)iter.ValueObject).value;
                        break;
                    }
                }
            }
            ClassicAssert.Greater(victimAddress, 0, "Did not find a record straddling a sector boundary");

            // The record still starts below the scan end, so the scan must return it whole, with its object read from the right object-log position.
            long lastAddress = 0, lastNextAddress = 0;
            var lastKey = -1;
            var lastValue = -1;
            using (var iter = store.Log.Scan(start, victimAddress + 8))
            {
                while (iter.GetNext())
                {
                    lastAddress = iter.CurrentAddress;
                    lastNextAddress = iter.NextAddress;
                    lastKey = iter.Key.AsRef<TestObjectKey>().key;
                    lastValue = ((TestObjectValue)iter.ValueObject).value;
                }
            }

            Assert.That(lastAddress, Is.EqualTo(victimAddress), "Scan returned the wrong last record");
            Assert.That(lastNextAddress, Is.EqualTo(victimNextAddress), "Scan did not advance past the whole record");
            Assert.That(lastKey, Is.EqualTo(victimKey), "Scan truncated the record's key");
            Assert.That(lastValue, Is.EqualTo(victimValue), "Scan truncated the record's object");
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]

        public void BlittableScanJumpToBeginAddressTest()
        {
            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "test.log"));
            objlog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "test.obj.log"));
            store = new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                ObjectLogDevice = objlog,
                MutableFraction = 0.1,
                LogMemorySize = 1L << 20,
                PageSize = 1L << 15,
                SegmentSize = 1L << 18
            }, StoreFunctions.Create(comparer, () => new TestObjectValue.Serializer())
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            using var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions());
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
                var key = new TestObjectKey { key = i };
                var value = new TestObjectValue { value = i };
                _ = bContext.Upsert(key, value, Empty.Default);
            }

            using var iter = store.Log.Scan(store.Log.HeadAddress, store.Log.TailAddress);

            for (int i = 0; i < 100; ++i)
            {
                ClassicAssert.IsTrue(iter.GetNext());
                ClassicAssert.AreEqual(i, iter.Key.AsRef<TestObjectKey>().key);
                ClassicAssert.AreEqual(i, ((TestObjectValue)iter.ValueObject).value);
            }

            store.Log.ShiftBeginAddress(shiftBeginAddressTo);

            for (int i = 0; i < numTailRecords; ++i)
            {
                ClassicAssert.IsTrue(iter.GetNext());
                if (i == 0)
                    ClassicAssert.AreEqual(store.Log.BeginAddress, iter.CurrentAddress);
                var expectedKey = numRecords - numTailRecords + i;
                ClassicAssert.AreEqual(expectedKey, iter.Key.AsRef<TestObjectKey>().key);
                ClassicAssert.AreEqual(expectedKey, ((TestObjectValue)iter.ValueObject).value);
            }
        }

        public class ScanFunctions : TestObjectFunctions
        {
            // Right now this is unused but helped with debugging so I'm keeping it around.
            internal long insertedAddress;

            public override bool InitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TestObjectInput input, IHeapObject srcValue, ref TestObjectOutput output, ref UpsertInfo upsertInfo)
            {
                insertedAddress = upsertInfo.Address;
                return base.InitialWriter(ref logRecord, in sizeInfo, ref input, srcValue, ref output, ref upsertInfo);
            }
        }

        sealed class KeySnapshotScanFunctions(bool overflowKey, bool evicted) : IScanIteratorFunctions
        {
            internal readonly HashSet<int> seen = [];
            public bool IncludeValues => false;
            public bool OnStart(long beginAddress, long endAddress) => true;
            public void OnStop(bool completed, long numberOfRecords) { }
            public void OnException(Exception exception, long numberOfRecords) { }

            public bool Reader<TSourceLogRecord>(in TSourceLogRecord record, RecordMetadata metadata, long numberOfRecords, out CursorRecordResult result)
                where TSourceLogRecord : ISourceLogRecord
            {
                result = CursorRecordResult.Accept;
                var index = BitConverter.ToInt32(record.Key);
                Assert.That(seen.Add(index), Is.True);
                Assert.That(record.Key.Length, Is.EqualTo(overflowKey ? OverflowKeyLength : sizeof(int)));
                if (evicted)
                    Assert.That(((TestObjectValue)record.ValueObject).value, Is.EqualTo(index));
                else
                {
                    Assert.That(record.DataHeader.ValueIsInline, Is.True);
                    Assert.That(record.ValueSpan.Length, Is.Zero);
                }
                return true;
            }
        }

        [Test]
        public void ObjectKeySnapshotsPreserveOverflowKeysAndSourceValues([Values] bool overflowKey, [Values] bool evicted, [Values] bool cursorScan)
        {
            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "key-snapshot.log"), deleteOnClose: true);
            objlog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "key-snapshot.obj.log"), deleteOnClose: true);
            store = new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                ObjectLogDevice = objlog,
                LogMemorySize = 1L << 20,
                PageSize = 1L << 15
            }, StoreFunctions.Create(comparer, () => new TestObjectValue.Serializer()),
                (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));
            using var session = store.NewSession<OverflowTestKey, TestObjectInput, TestObjectOutput, Empty, ScanFunctions>(new ScanFunctions());
            var basic = session.BasicContext;
            const int count = 128;
            for (var i = 0; i < count; i++)
                _ = basic.Upsert(new OverflowTestKey(i, overflowKey ? OverflowKeyLength : sizeof(int)), new TestObjectValue { value = i });
            if (evicted)
                store.Log.FlushAndEvict(wait: true);
            var scanner = new KeySnapshotScanFunctions(overflowKey, evicted);
            if (cursorScan)
            {
                long cursor = 0;
                while (session.ScanCursor(ref cursor, 7, scanner, store.Log.TailAddress))
                    Assert.That(scanner.seen.Count, Is.LessThanOrEqualTo(count));
            }
            else
                Assert.That(store.Log.Scan(ref scanner, store.Log.BeginAddress, store.Log.TailAddress), Is.True);
            Assert.That(scanner.seen.Count, Is.EqualTo(count));
            for (var i = 0; i < count; i++)
            {
                TestObjectInput input = default;
                TestObjectOutput output = default;
                var status = basic.Read(new OverflowTestKey(i, overflowKey ? OverflowKeyLength : sizeof(int)), ref input, ref output);
                if (status.IsPending)
                {
                    _ = basic.CompletePendingWithOutputs(out var completed, wait: true);
                    Assert.That(completed.Next(), Is.True);
                    output = completed.Current.Output;
                    status = completed.Current.Status;
                    completed.Dispose();
                }
                Assert.That(status.Found, Is.True);
                Assert.That(output.value.value, Is.EqualTo(i));
            }
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]

        public void ObjectScanCursorTest([Values(HashModulo.NoMod, HashModulo.Hundred)] HashModulo hashMod)
        {
            const int PageSizeBits = 9;
            const long PageSize = 1L << PageSizeBits;

            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "test.log"));
            objlog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "test.obj.log"));

            store = new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                ObjectLogDevice = objlog,
                MutableFraction = 0.1,
                LogMemorySize = 1L << 20,
                PageSize = 1L << 15,
                SegmentSize = 1L << 18
            }, StoreFunctions.Create(comparer, () => new TestObjectValue.Serializer())
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            using var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, ScanFunctions>(new ScanFunctions());
            var bContext = session.BasicContext;

            var startTailAddress = store.Log.TailAddress;
            var recordSize = 0;
            for (int i = 0; i < TotalRecords; i++)
            {
                var key1 = new TestObjectKey { key = i };
                var value = new TestObjectValue { value = i };
                _ = bContext.Upsert(key1, value);

                if (recordSize == 0)
                {
                    // Verify the recordSize from the first record's tailAddress growth.
                    recordSize = (int)(store.Log.TailAddress - startTailAddress);
                    // Size should be RecordInfo, RecordDataHeader.Size, Key len 4, value size 4 (objectId), objectLogPosition ulong.
                    Assert.That(recordSize, Is.EqualTo(32), $"Expected record size of 32 but was {recordSize}");
                }
            }

            var scanCursorFuncs = new ScanCursorFuncs();

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
            ClassicAssert.IsFalse(session.ScanCursor(ref cursor, count: long.MaxValue, scanCursorFuncs, endAddress: long.MaxValue), "Expected scan to finish and return false, pt 1");
            ClassicAssert.AreEqual(TotalRecords, scanCursorFuncs.numRecords, "Unexpected count for all on-disk");
            ClassicAssert.AreEqual(0, cursor, "Expected cursor to be 0, pt 2");

            // Add another totalRecords, with keys incremented by totalRecords to remain distinct, and verify we see all keys.
            for (int i = 0; i < TotalRecords; i++)
            {
                var key1 = new TestObjectKey { key = i + TotalRecords };
                var value = new TestObjectValue { value = i + TotalRecords };
                _ = bContext.Upsert(key1, value);
            }
            scanCursorFuncs.Initialize(verifyKeys);
            ClassicAssert.IsFalse(session.ScanCursor(ref cursor, long.MaxValue, scanCursorFuncs, long.MaxValue), "Expected scan to finish and return false, pt 1");
            ClassicAssert.AreEqual(TotalRecords * 2, scanCursorFuncs.numRecords, "Unexpected count for on-disk + in-mem");
            ClassicAssert.AreEqual(0, cursor, "Expected cursor to be 0, pt 3");

            // Try an invalid cursor (not a multiple of 8) on-disk and verify we get one correct record. Use 3x page size to make sure page boundaries are tested.
            ClassicAssert.Greater(store.hlogBase.GetTailAddress(), PageSize * 10, "Need enough space to exercise this");
            scanCursorFuncs.Initialize(verifyKeys);
            cursor = store.hlogBase.BeginAddress - 1;
            do
            {
                ClassicAssert.IsTrue(session.ScanCursor(ref cursor, 1, scanCursorFuncs, long.MaxValue, validateCursor: true), "Expected scan to finish and return false, pt 1");
                cursor = scanCursorFuncs.lastAddress + recordSize + 1;
            } while (cursor < PageSize * 3);

            // Now try an invalid cursor in-memory. First we have to read what's at the target start address (let's use HeadAddress) to find what the value is.
            TestObjectInput input = new();
            TestObjectOutput output = new();
            ReadOptions readOptions = default;
            var readStatus = bContext.ReadAtAddress(store.hlogBase.HeadAddress, ref input, ref output, ref readOptions, out _);
            ClassicAssert.IsTrue(readStatus.Found, $"Could not read at HeadAddress; {readStatus}");

            scanCursorFuncs.Initialize(verifyKeys);
            scanCursorFuncs.numRecords = output.value.value;
            cursor = store.Log.HeadAddress + 1;
            do
            {
                ClassicAssert.IsTrue(session.ScanCursor(ref cursor, 1, scanCursorFuncs, long.MaxValue, validateCursor: true), "Expected scan to finish and return false, pt 1");
                cursor = scanCursorFuncs.lastAddress + recordSize + 1;
            } while (cursor < store.hlogBase.HeadAddress + PageSize * 3);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]

        public void ObjectScanCursorFilterTest([Values(HashModulo.NoMod, HashModulo.Hundred)] HashModulo hashMod)
        {
            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "test.log"));
            objlog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "test.obj.log"));

            store = new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                ObjectLogDevice = objlog,
                MutableFraction = 0.1,
                LogMemorySize = 1L << 20,
                PageSize = 1L << 15,
                SegmentSize = 1L << 18
            }, StoreFunctions.Create(comparer, () => new TestObjectValue.Serializer())
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            using var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, ScanFunctions>(new ScanFunctions());
            var bContext = session.BasicContext;

            for (int i = 0; i < TotalRecords; i++)
            {
                var key1 = new TestObjectKey { key = i };
                var value = new TestObjectValue { value = i };
                _ = bContext.Upsert(key1, value);
            }

            var scanCursorFuncs = new ScanCursorFuncs();

            long cursor = 0;
            scanCursorFuncs.Initialize(verifyKeys: false, k => k.key % 10 == 0);
            ClassicAssert.IsTrue(session.ScanCursor(ref cursor, 10, scanCursorFuncs, store.Log.TailAddress), "ScanCursor failed, pt 1");
            ClassicAssert.AreEqual(10, scanCursorFuncs.numRecords, "count at first 10");
            ClassicAssert.Greater(cursor, 0, "Expected cursor to be > 0, pt 1");

            // Now fake out the key verification to make it think we got all the previous keys; this ensures we are aligned as expected.
            scanCursorFuncs.Initialize(verifyKeys: true, k => true);
            scanCursorFuncs.numRecords = 91;   // (filter accepts: 0-9) * 10 + 1
            ClassicAssert.IsTrue(session.ScanCursor(ref cursor, 100, scanCursorFuncs, store.Log.TailAddress), "ScanCursor failed, pt 2");
            ClassicAssert.AreEqual(191, scanCursorFuncs.numRecords, "count at second 100");
            ClassicAssert.Greater(cursor, 0, "Expected cursor to be > 0, pt 1");
        }

        sealed class RecordingValueSerializer(ConcurrentQueue<WeakReference<IHeapObject>> objects) : TestObjectValue.Serializer
        {
            public override void Deserialize(out IHeapObject obj)
            {
                base.Deserialize(out obj);
                objects.Enqueue(new(obj));
            }
        }

        ConcurrentQueue<WeakReference<IHeapObject>> CreateRecordingScanStore()
        {
            var objects = new ConcurrentQueue<WeakReference<IHeapObject>>();
            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "scan.log"), deleteOnClose: true);
            objlog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "scan.obj.log"), deleteOnClose: true);
            store = new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                ObjectLogDevice = objlog,
                LogMemorySize = 1L << 20,
                PageSize = MinKvLogPageSize
            }, StoreFunctions.Create(comparer, () => new RecordingValueSerializer(objects)),
                (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));
            var tracker = new LogSizeTracker<ClassStoreFunctions, ClassAllocator>(store.Log, 1L << 20, 1L << 17, 1L << 16, logger: null);
            store.Log.SetLogSizeTracker(tracker);
            using var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, ScanFunctions>(new ScanFunctions());
            for (var i = 0; i < 2200; i++)
                _ = session.BasicContext.Upsert(new TestObjectKey { key = i }, new TestObjectValue { value = i });
            store.Log.FlushAndEvict(wait: true);
            Assert.That(tracker.LogHeapSizeBytes, Is.Zero);
            return objects;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        static void AssertDeserializedObjectsReleased(ConcurrentQueue<WeakReference<IHeapObject>> objects)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            Assert.That(objects, Is.Not.Empty, "The scan must actually deserialize disk objects");
            foreach (var reference in objects)
                Assert.That(reference.TryGetTarget(out _), Is.False, "A disposed scan still roots a deserialized object");
            objects.Clear();
        }

        [Test]
        [Category("TsavoriteKV")]
        public void ObjectScanCursorReleasesFrameObjects([Values(1L, 100L, long.MaxValue)] long count)
        {
            var objects = CreateRecordingScanStore();
            using var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, ScanFunctions>(new ScanFunctions());
            var functions = new ScanCursorFuncs();
            functions.Initialize(verifyKeys: false, k => (k.key & 1) != 0);
            long cursor = 0;
            for (var i = 0; i < 4; i++)
            {
                _ = session.ScanCursor(ref cursor, count, functions);
                AssertDeserializedObjectsReleased(objects);
            }
        }

        [Test]
        [Category("TsavoriteKV")]
        public void ObjectScanDoesNotChargeTransientHeap([Values(1L, 100L, long.MaxValue)] long count)
        {
            _ = CreateRecordingScanStore();
            using var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, ScanFunctions>(new ScanFunctions());
            var functions = new ScanCursorFuncs();
            functions.Initialize(verifyKeys: false);
            long cursor = 0;
            for (var i = 0; i < 4; i++)
            {
                _ = session.ScanCursor(ref cursor, count, functions);
                Assert.That(store.Log.HeapSizeBytes, Is.Zero, "Scan frames are not resident log heap");
            }
        }

        [Test]
        [Category("TsavoriteKV")]
        public void ObjectPullScanReleasesFrameObjects(
            [Values(DiskScanBufferingMode.SinglePageBuffering, DiskScanBufferingMode.DoublePageBuffering)] DiskScanBufferingMode buffering,
            [Values(1, int.MaxValue)] int recordsToRead)
        {
            var objects = CreateRecordingScanStore();
            for (var i = 0; i < 3; i++)
            {
                ReadScanRecords(buffering, recordsToRead);
                AssertDeserializedObjectsReleased(objects);
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        void ReadScanRecords(DiskScanBufferingMode buffering, int recordsToRead)
        {
            using var iter = store.Log.Scan(store.Log.BeginAddress, store.Log.TailAddress, buffering);
            for (var n = 0; n < recordsToRead && iter.GetNext(); n++)
                Assert.That(iter.ValueObject, Is.InstanceOf<TestObjectValue>());
        }

        [Test]
        [Category("TsavoriteKV")]
        public void ObjectScanFrameCleanupReleasesOverflowKeys()
        {
            var objects = CreateRecordingScanStore();
            var fromAddress = store.Log.TailAddress;
            using (var session = store.NewSession<OverflowTestKey, TestObjectInput, TestObjectOutput, Empty, ScanFunctions>(new ScanFunctions()))
            {
                for (var i = 0; i < 100; i++)
                    _ = session.BasicContext.Upsert(new OverflowTestKey(i + 2200), new TestObjectValue { value = i });
            }
            store.Log.FlushAndEvict(wait: true);
            var overflow = ReadOverflowScanRecord(fromAddress);
            AssertDeserializedObjectsReleased(objects);
            Assert.That(overflow.TryGetTarget(out _), Is.False, "Disposal must release overflow key slots as well as objects");
            Assert.That(store.Log.HeapSizeBytes, Is.Zero);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        WeakReference<byte[]> ReadOverflowScanRecord(long fromAddress)
        {
            using var iter = store.Log.Scan(fromAddress, store.Log.TailAddress, DiskScanBufferingMode.SinglePageBuffering);
            Assert.That(iter.GetNext(), Is.True);
            Assert.That(((TestObjectValue)iter.ValueObject).value, Is.Zero);
            Assert.That(iter.Key.Length, Is.EqualTo(OverflowKeyLength));
            return new(iter.KeyOverflow.Array);
        }

        [Test]
        [Category("TsavoriteKV")]
        public void ObjectScanFramesDoNotShareOwnership()
        {
            var objects = CreateRecordingScanStore();
            ReadOverlappingScanFrames();
            AssertDeserializedObjectsReleased(objects);
        }

        [Test]
        [Category("TsavoriteKV")]
        public void ObjectScanFramesShareIteratorMap(
            [Values(DiskScanBufferingMode.SinglePageBuffering, DiskScanBufferingMode.DoublePageBuffering)] DiskScanBufferingMode buffering)
        {
            _ = CreateRecordingScanStore();
            using var iter = store.Log.Scan(store.Log.BeginAddress, store.Log.TailAddress, buffering);
            ObjectIdMap map = null;
            var count = 0;
            while (iter.GetNext())
            {
                map ??= iter.ObjectIdMap;
                Assert.That(iter.ObjectIdMap, Is.SameAs(map), "Every disk frame must use the iterator's map");
                Assert.That(((TestObjectValue)iter.ValueObject).value, Is.EqualTo(count++));
            }
            Assert.That(count, Is.EqualTo(2200), "The scan must cross and reuse multiple frames");
            var frameCount = buffering == DiskScanBufferingMode.DoublePageBuffering ? 2 : 1;
            Assert.That(map.Count, Is.LessThanOrEqualTo(frameCount * store.Log.PageSizeBytes / 32),
                "Released slots must be reused rather than accumulated across pages");
        }

        [Test]
        [Category("TsavoriteKV")]
        public void TransferredScanRecordOutlivesIterator([Values(false, true)] bool fromDisk)
        {
            _ = CreateRecordingScanStore();
            var fromAddress = store.Log.BeginAddress;
            if (!fromDisk)
            {
                fromAddress = store.Log.TailAddress;
                using var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, ScanFunctions>(new ScanFunctions());
                _ = session.BasicContext.Upsert(new TestObjectKey { key = 2200 }, new TestObjectValue { value = 2200 });
            }
            using var iter = store.Log.Scan(fromAddress, store.Log.TailAddress, DiskScanBufferingMode.SinglePageBuffering);
            Assert.That(iter.GetNext(), Is.True);
            var value = iter.ValueObject;
            var transferred = DiskLogRecord.TransferFrom(ref iter.AsDiskLogRecordRef(), store.hlogBase.bufferPool, store.Log.TransientObjectIdMap);
            try
            {
                Assert.That(iter.IsSet, Is.False, "Transfer must relinquish the iterator's current record");
                iter.Dispose();
                Assert.That(transferred.ValueObject, Is.SameAs(value), "Transferred slots must survive iterator map cleanup");

                // Transferring an owned buffer must move it intact, without clearing its slots.
                var physicalAddress = transferred.logRecord.physicalAddress;
                var moved = DiskLogRecord.TransferFrom(ref transferred, store.hlogBase.bufferPool, store.Log.TransientObjectIdMap);
                try
                {
                    Assert.That(transferred.IsSet, Is.False);
                    Assert.That(moved.logRecord.physicalAddress, Is.EqualTo(physicalAddress));
                    Assert.That(moved.ValueObject, Is.SameAs(value));
                }
                finally
                {
                    moved.Dispose();
                }
                if (!fromDisk)
                    Assert.That(store.hlogBase._wrapper.CreateLogRecord(fromAddress).ValueObject, Is.SameAs(value),
                        "Releasing remapped slots must not change the resident record");
            }
            finally
            {
                transferred.Dispose();
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        void ReadOverlappingScanFrames()
        {
            using var first = store.Log.Scan(store.Log.BeginAddress, store.Log.TailAddress, DiskScanBufferingMode.SinglePageBuffering);
            Assert.That(first.GetNext(), Is.True);
            var firstValue = first.ValueObject;
            using (var second = store.Log.Scan(store.Log.BeginAddress, store.Log.TailAddress, DiskScanBufferingMode.DoublePageBuffering))
                Assert.That(second.GetNext(), Is.True);
            Assert.That(first.ValueObject, Is.SameAs(firstValue), "Disposing one iterator must not clear another iterator's objects");
        }

        sealed class DisposalCountingValue : TestObjectValue
        {
            internal int disposeCount;
            internal bool diskReadInitialized;
            public override void Dispose() => ++disposeCount;
        }

        sealed class DisposalCountingSerializer(ConcurrentQueue<DisposalCountingValue> values) : TestObjectValue.Serializer
        {
            public override void Deserialize(out IHeapObject obj)
            {
                var value = new DisposalCountingValue { value = reader.ReadInt32() };
                values.Enqueue(value);
                obj = value;
            }
        }

        internal sealed class DisposingScanTriggers : IRecordTriggers
        {
            internal int? throwOnValue;
            public bool CallOnDiskRead => true;
            public void OnDiskRead(ref LogRecord logRecord)
            {
                if (logRecord.DataHeader.ValueIsObject)
                    ((DisposalCountingValue)logRecord.ValueObject).diskReadInitialized = true;
            }
            public void OnDisposeDiskRecord(ref DiskLogRecord logRecord, DisposeReason reason)
            {
                if (!logRecord.IsSet || !logRecord.DataHeader.ValueIsObject)
                    return;
                var value = (DisposalCountingValue)logRecord.ValueObject;
                Assert.That(value.diskReadInitialized, Is.True, "Initialize disk-owned handles before invoking disposal");
                value.Dispose();
                if (value.value == throwOnValue)
                    throw new InvalidOperationException("Injected disk disposal failure");
            }
        }

        [Test]
        [Category("TsavoriteKV")]
        public void ObjectScanDisposesOwnedObjectsButNotResidentObjects(
            [Values(1L, long.MaxValue)] long count,
            [Values(HashModulo.NoMod, HashModulo.Hundred)] HashModulo hashMod)
        {
            var values = new ConcurrentQueue<DisposalCountingValue>();
            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "dispose.log"), deleteOnClose: true);
            objlog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "dispose.obj.log"), deleteOnClose: true);
            using var scanStore = new TsavoriteKV<DisposingStoreFunctions, DisposingAllocator>(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                ObjectLogDevice = objlog,
                LogMemorySize = 1L << 20,
                PageSize = MinKvLogPageSize
            }, StoreFunctions.Create(comparer, () => new DisposalCountingSerializer(values), new DisposingScanTriggers()),
                (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));
            using var session = scanStore.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, ScanFunctions>(new ScanFunctions());
            var residentValues = new DisposalCountingValue[2200];
            for (var i = 0; i < residentValues.Length; i++)
            {
                residentValues[i] = new() { value = i };
                _ = session.BasicContext.Upsert(new TestObjectKey { key = i }, residentValues[i]);
            }
            var functions = new ScanCursorFuncs();
            functions.Initialize(verifyKeys: false);
            long cursor = 0;
            _ = session.ScanCursor(ref cursor, long.MaxValue, functions);
            foreach (var value in residentValues)
                Assert.That(value.disposeCount, Is.Zero, "Buffered resident values are borrowed, not owned");

            scanStore.Log.FlushAndEvict(wait: true);
            cursor = 0;
            for (var i = 0; i < 4; i++)
            {
                _ = session.ScanCursor(ref cursor, count, functions);
                Assert.That(values, Is.Not.Empty);
                foreach (var value in values)
                    Assert.That(value.disposeCount, Is.EqualTo(1), "Every deserialized value must be disposed exactly once");
                values.Clear();
            }
        }

        [Test]
        [Category("TsavoriteKV")]
        public void ObjectScanFrameCleanupSurfacesDisposalFailureAndReleasesOtherObjects()
        {
            var values = new ConcurrentQueue<DisposalCountingValue>();
            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "failure.log"), deleteOnClose: true);
            objlog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "failure.obj.log"), deleteOnClose: true);
            var triggers = new DisposingScanTriggers();
            using var scanStore = new TsavoriteKV<DisposingStoreFunctions, DisposingAllocator>(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                ObjectLogDevice = objlog,
                LogMemorySize = 1L << 20,
                PageSize = MinKvLogPageSize
            }, StoreFunctions.Create(comparer, () => new DisposalCountingSerializer(values), triggers),
                (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));
            using var session = scanStore.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, ScanFunctions>(new ScanFunctions());
            for (var i = 0; i < 300; i++)
                _ = session.BasicContext.Upsert(new TestObjectKey { key = i }, new DisposalCountingValue { value = i });
            scanStore.Log.FlushAndEvict(wait: true);
            var iter = scanStore.Log.Scan(scanStore.Log.BeginAddress, scanStore.Log.TailAddress, DiskScanBufferingMode.DoublePageBuffering);
            try
            {
                Assert.That(iter.GetNext(), Is.True);
                // Fail on an unvisited record in the first frame; the second frame's read-ahead also needs cleanup.
                triggers.throwOnValue = 1;
                Assert.That(() => iter.Dispose(), Throws.InvalidOperationException.With.Message.EqualTo("Injected disk disposal failure"));
                Assert.That(values.Count, Is.GreaterThan(126), "Read-ahead must deserialize another page");
                foreach (var value in values)
                    Assert.That(value.disposeCount, Is.EqualTo(1));
            }
            finally
            {
                triggers.throwOnValue = null;
                iter.Dispose();
            }
        }

        internal sealed class ScanCursorFuncs : IScanIteratorFunctions
        {
            internal int numRecords;
            internal long lastAddress;
            internal bool verifyKeys;
            internal Func<TestObjectKey, bool> filter;

            internal void Initialize(bool verifyKeys) => Initialize(verifyKeys, k => true);

            internal void Initialize(bool verifyKeys, Func<TestObjectKey, bool> filter)
            {
                numRecords = 0;
                this.verifyKeys = verifyKeys;
                this.filter = filter;
            }

            public bool Reader<TSourceLogRecord>(in TSourceLogRecord logRecord, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                where TSourceLogRecord : ISourceLogRecord
            {
                cursorRecordResult = filter(logRecord.Key.AsRef<TestObjectKey>()) ? CursorRecordResult.Accept : CursorRecordResult.Skip;
                if (cursorRecordResult != CursorRecordResult.Accept)
                    return true;

                if (verifyKeys)
                    ClassicAssert.AreEqual(numRecords, logRecord.Key.AsRef<TestObjectKey>().key, "Mismatched key field on Scan");
                Assert.That(((TestObjectValue)logRecord.ValueObject).value, Is.EqualTo(logRecord.Key.AsRef<TestObjectKey>().key), "Mismatched value on Scan");
                ClassicAssert.Greater(recordMetadata.Address, 0);
                ++numRecords;
                lastAddress = recordMetadata.Address;
                return true;
            }

            public void OnException(Exception exception, long numberOfRecords)
                => Assert.Fail($"Unexpected exception at {numberOfRecords} records: {exception.Message}");

            public bool OnStart(long beginAddress, long endAddress) => true;

            public void OnStop(bool completed, long numberOfRecords) { }
        }
    }
}