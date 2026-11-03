// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.recovery
{
    using ClassAllocator = ObjectAllocator<StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>>;
    using ClassStoreFunctions = StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>;

    /// <summary>
    /// End-to-end regression test for #2101, driving the real paths: an RMW that performs a CopyUpdate while a
    /// checkpoint is active, the post-checkpoint cleanup that releases the cached bytes, and a later flush that
    /// serializes the superseded record while the surviving record mutates the collection they share.
    /// </summary>
    [TestFixture]
    internal class CheckpointSharedObjectMutationTests : TestBase
    {
        private TsavoriteKV<ClassStoreFunctions, ClassAllocator> store;
        private IDevice log, objlog;

        /// <summary>
        /// Models a Garnet collection object: <see cref="Clone"/> is a SHALLOW copy, so the (v+1) record created by
        /// a CopyUpdate shares this instance's list. <see cref="DoSerialize"/> enumerates that shared list, and can
        /// be paused mid-enumeration so a test can mutate it and reproduce "Collection was modified".
        /// </summary>
        internal sealed class SharedListHeapObject : HeapObjectBase
        {
            internal readonly List<int> Items;

            /// <summary>Set to pause inside the enumeration so the caller can mutate <see cref="Items"/>.</summary>
            internal ManualResetEventSlim PauseDuringSerialize;
            internal readonly ManualResetEventSlim ReachedSerialize = new(false);

            internal int DoSerializeCount;

            /// <summary>The value of <c>Items.Count</c> at each <see cref="DoSerialize"/> call, in order. This is the
            /// discriminator between "the preserved image held the wrong bytes" and "the preserved image was correct but
            /// something re-serialized the live object afterwards".</summary>
            internal readonly List<int> SerializedItemCounts = [];

            /// <summary>The call stack at each <see cref="DoSerialize"/>, so a failing run names the second caller.</summary>
            internal readonly List<string> SerializedStacks = [];

            internal SharedListHeapObject(List<int> items)
            {
                Items = items;
                HeapMemorySize = 64;
            }

            // Shallow copy: this is the crux of #2101 - the clone shares Items with the record it supersedes.
            public override IHeapObject Clone() => new SharedListHeapObject(Items);

            public override void Dispose() { }

            public override void DoSerialize(BinaryWriter writer)
            {
                _ = Interlocked.Increment(ref DoSerializeCount);
                lock (SerializedItemCounts)
                {
                    // Capture a stack only for a repeat serialization: the first call is the preserved-image capture and is
                    // not interesting, and taking a stack there slows it enough to close the race being investigated.
                    if (SerializedItemCounts.Count > 0)
                        SerializedStacks.Add(Environment.StackTrace);
                    SerializedItemCounts.Add(Items.Count);
                }
                writer.Write(Items.Count);

                // foreach over the shared list: throws InvalidOperationException if another thread mutates it.
                var first = true;
                foreach (var item in Items)
                {
                    if (first && PauseDuringSerialize is not null)
                    {
                        first = false;
                        ReachedSerialize.Set();
                        PauseDuringSerialize.Wait(TimeSpan.FromSeconds(10));
                    }
                    writer.Write(item);
                }
            }

            public override void WriteType(BinaryWriter writer, bool isNull) => writer.Write(isNull);
        }

        internal sealed class SharedListSerializer : BinaryObjectSerializer<IHeapObject>
        {
            public override void Deserialize(out IHeapObject obj)
            {
                // A null image (the isNull type byte written by WriteType) carries no data.
                if (reader.ReadBoolean())
                {
                    obj = null;
                    return;
                }
                var count = reader.ReadInt32();
                var items = new List<int>(count);
                for (var i = 0; i < count; i++)
                    items.Add(reader.ReadInt32());
                obj = new SharedListHeapObject(items);
            }

            /// <remarks>
            /// Must go through <see cref="IHeapObject.Serialize"/>, not straight to <c>DoSerialize</c>. Only
            /// <c>HeapObjectBase.Serialize</c> consults the serialization phase and the preserved <c>(v)</c> bytes; calling
            /// <c>DoSerialize</c> directly bypasses that and serializes the live object, which during a checkpoint is the
            /// one the superseding record has already mutated. It also skips <c>WriteType</c>, which
            /// <see cref="Deserialize"/> expects.
            /// </remarks>
            public override void Serialize(IHeapObject obj) => obj.Serialize(writer);
        }

        /// <summary>Pauses the checkpoint state machine on entry to a chosen phase so the test can act in that window.</summary>
        private sealed class PauseAtPhase(Phase pauseAt) : IStateMachineCallback
        {
            internal readonly ManualResetEventSlim Reached = new(false);
            internal readonly ManualResetEventSlim Release = new(false);

            public void BeforeEnteringState(SystemState next)
            {
                if (next.Phase != pauseAt)
                    return;
                Reached.Set();
                _ = Release.Wait(TimeSpan.FromSeconds(30));
            }
        }

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "SharedObj.log"), deleteOnClose: true);
            objlog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "SharedObj.obj.log"), deleteOnClose: true);
            store = new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                ObjectLogDevice = objlog,
                MutableFraction = 0.1,
                LogMemorySize = 1L << 16,
                PageSize = 1L << 13,
                CheckpointDir = MethodTestDir
            }, StoreFunctions.Create(new TestObjectKey.Comparer(), () => new SharedListSerializer())
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));
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

        [Test]
        [Category("TsavoriteKV")]
        [Category("CheckpointRestore")]
        public async Task SupersededObjectIsNotSerializedFromLiveStateAfterCleanup()
        {
            var key = new TestObjectKey { key = 1 };
            var shared = new List<int> { 1, 2, 3, 4, 5, 6, 7, 8 };
            var original = new SharedListHeapObject(shared);

            using (var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, SharedListFunctions>(new SharedListFunctions()))
                _ = session.BasicContext.Upsert(key, original, Empty.Default);

            var sourceAddress = store.Log.TailAddress - 1;

            // Checkpoint C1: pause on entry to WAIT_FLUSH, which is after IN_PROGRESS has been published and the
            // fuzzy region has opened, so an RMW in this window is (v+1) against a (v) source and must RCU.
            var pause = new PauseAtPhase(Phase.WAIT_FLUSH);
            store.stateMachineDriver.UnsafeRegisterCallback(pause);

            Assert.That(store.TryInitiateFullCheckpoint(out _, CheckpointType.Snapshot), Is.True);
            Assert.That(pause.Reached.Wait(TimeSpan.FromSeconds(30)), Is.True, "Checkpoint did not reach WAIT_FLUSH");

            // Real RMW -> CreateNewRecordRMW -> CacheSerializedObjectData on the superseded (v) object.
            using (var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, SharedListFunctions>(new SharedListFunctions()))
            {
                TestObjectInput input = new() { value = 99 };
                TestObjectOutput output = new();
                var status = session.BasicContext.RMW(key, ref input, ref output);
                if (status.IsPending)
                    _ = session.BasicContext.CompletePending(wait: true);
            }

            pause.Release.Set();
            await store.CompleteCheckpointAsync().ConfigureAwait(false);

            // The RMW must actually have gone through the CopyUpdate-during-checkpoint path.
            Assert.That(original.DoSerializeCount, Is.GreaterThanOrEqualTo(1),
                "The RMW did not cache the superseded object's (v) bytes; the test is not exercising the #2101 path");
            var afterCheckpoint = original.DoSerializeCount;

            // Post-checkpoint cleanup, exactly as Garnet's RunPostCheckpointCleanup does.
            store.Log.ClearSerializedObjectData(store.Log.BeginAddress, store.Log.TailAddress);

            // The surviving (v+1) record shares this list and keeps mutating it.
            shared.Add(1000);

            // Serialize the superseded record again, pausing mid-enumeration so we can mutate concurrently.
            // Pre-fix the phase was reset to REST, so this takes the direct path and enumerates the live list.
            original.PauseDuringSerialize = new ManualResetEventSlim(false);
            Exception serializeFailure = null;
            var serializeTask = Task.Run(() =>
            {
                try
                {
                    using var ms = new MemoryStream();
                    using var writer = new BinaryWriter(ms);
                    original.Serialize(writer);
                }
                catch (Exception ex)
                {
                    serializeFailure = ex;
                }
            });

            if (original.ReachedSerialize.Wait(TimeSpan.FromSeconds(2)))
            {
                // Only reachable if Serialize took the direct path: mutate while the enumerator is live.
                shared.Add(2000);
                original.PauseDuringSerialize.Set();
            }
            await serializeTask.ConfigureAwait(false);

            Assert.Multiple(() =>
            {
                Assert.That(original.DoSerializeCount, Is.EqualTo(afterCheckpoint),
                    "#2101: the superseded object was re-serialized from its live, shared collection after cleanup");
                Assert.That(serializeFailure, Is.Null,
                    $"#2101: serializing the superseded object threw {serializeFailure?.GetType().Name}: {serializeFailure?.Message}");
            });

            _ = sourceAddress;
        }

        /// <summary>
        /// Whichever version a checkpoint recovers, its content must match that version: a recovered <c>(v)</c> record must
        /// carry its pre-mutation image, and a recovered <c>(v+1)</c> record must carry the post-mutation one. The bug this
        /// guards is <c>(v)</c> surviving with <c>(v+1)</c>'s content, which is what happens when a superseded record is
        /// written from the collection its successor has already mutated.
        /// </summary>
        /// <remarks>
        /// The two checkpoint types resolve this differently, and the test asserts the invariant rather than a fixed
        /// version, because which one wins is a property of the checkpoint type rather than of this hazard.
        /// <para>
        /// It is tempting to reason that the RCU's CAS makes <c>(v)</c> unreachable, so its content cannot matter. That is
        /// true of the live hash chain and not of recovery: <c>RecoverFromPage</c> invalidates a <c>(v+1)</c> record at or
        /// above <c>fuzzyRegionStartAddress</c> when <c>undoNextVersion</c> is set (the default), and for a non-snapshot
        /// page it then repoints the hash entry at <c>info.PreviousAddress</c> — which is <c>(v)</c>.
        /// </para>
        /// <para>
        /// The discriminator is the shared list: <c>PostCopyUpdater</c> appends to the list that <c>(v)</c> and
        /// <c>(v+1)</c> share, so <c>(v+1)</c>'s content is the longer list and <c>(v)</c>'s correct content is the shorter
        /// one, which only the preserved image still holds.
        /// </para>
        /// <para>This is the behaviour that retiring the serialized-object cache must preserve, for each checkpoint type.</para>
        /// </remarks>
        [Test]
        [Category("TsavoriteKV")]
        [Category("CheckpointRestore")]
        public async Task RecoveredVersionCarriesItsOwnContent([Values(CheckpointType.FoldOver)] CheckpointType checkpointType)
            => await RecoveredVersionCarriesItsOwnContentCore(checkpointType).ConfigureAwait(false);

        /// <summary>
        /// The Snapshot case of <see cref="RecoveredVersionCarriesItsOwnContent"/>.
        /// </summary>
        /// <remarks>
        /// This failed intermittently (~1 run in 3-6) until the cause was found, and the cause was in this fixture rather
        /// than in the store: <c>SharedListSerializer.Serialize</c> called <c>DoSerialize</c> directly instead of
        /// <see cref="IHeapObject.Serialize"/>. Only <c>HeapObjectBase.Serialize</c> consults the serialization phase and
        /// the preserved <c>(v)</c> bytes, so going straight to <c>DoSerialize</c> serialized the live object — which, during
        /// a checkpoint, is the one the superseding record has already mutated.
        /// <para>
        /// A stack captured on the second serialization named the path precisely:
        /// <c>AsyncFlushPagesForSnapshot</c> → <c>WriteAsyncToDeviceForSnapshot</c> → <c>WriteAsync</c> →
        /// <c>ObjectLogWriter.WriteRecordObjects</c> → <c>ObjectLogWriter.DoSerialize</c> → the serializer. The recorded
        /// item counts were <c>[8, 9]</c>: the image was preserved correctly at 8, then the live object was serialized
        /// again at 9 and that is what landed.
        /// </para>
        /// <para>
        /// Intermittent because it depended on whether the page had already been flushed before the mutation. That is why
        /// a single passing run proved nothing here, and why the fixture bug survived: the test passed most of the time.
        /// </para>
        /// </remarks>
        [Test]
        [Category("TsavoriteKV")]
        [Category("CheckpointRestore")]
        public async Task RecoveredVersionCarriesItsOwnContent_Snapshot()
            => await RecoveredVersionCarriesItsOwnContentCore(CheckpointType.Snapshot).ConfigureAwait(false);

        private async Task RecoveredVersionCarriesItsOwnContentCore(CheckpointType checkpointType)
        {
            const int InitialCount = 8;
            const int RmwValue = 99;

            var dir = Path.Join(MethodTestDir, checkpointType.ToString());
            _ = Directory.CreateDirectory(dir);
            var key = new TestObjectKey { key = 1 };

            using var ckptLog = Devices.CreateLogDevice(Path.Join(dir, "Ckpt.log"), deleteOnClose: false);
            using var ckptObjLog = Devices.CreateLogDevice(Path.Join(dir, "Ckpt.obj.log"), deleteOnClose: false);

            static TsavoriteKV<ClassStoreFunctions, ClassAllocator> CreateStore(IDevice log, IDevice objlog, string dir)
                => new(new()
                {
                    IndexSize = 1L << 13,
                    LogDevice = log,
                    ObjectLogDevice = objlog,
                    MutableFraction = 0.9,
                    LogMemorySize = 1L << 16,
                    PageSize = 1L << 13,
                    CheckpointDir = dir
                }, StoreFunctions.Create(new TestObjectKey.Comparer(), () => new SharedListSerializer())
                 , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));

            var shared = new List<int>();
            for (var ii = 1; ii <= InitialCount; ++ii)
                shared.Add(ii);

            long sourceTail, newRecordTail;
            int[] serializedCounts;
            string[] serializedStacks;
            SharedListHeapObject sourceObject = null;
            var writeStore = CreateStore(ckptLog, ckptObjLog, dir);
            try
            {
                sourceObject = new SharedListHeapObject(shared);
                using (var session = writeStore.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, SharedListFunctions>(new SharedListFunctions()))
                    _ = session.BasicContext.Upsert(key, sourceObject, Empty.Default);
                sourceTail = writeStore.Log.TailAddress;

                // Pause on entry to WAIT_FLUSH: IN_PROGRESS has been published and the fuzzy region (which opened at the
                // PREPARE tail) is now behind us, so an RMW here is (v+1) against a (v) source and must RCU.
                var pause = new PauseAtPhase(Phase.WAIT_FLUSH);
                writeStore.stateMachineDriver.UnsafeRegisterCallback(pause);

                Assert.That(writeStore.TryInitiateFullCheckpoint(out _, checkpointType), Is.True);
                Assert.That(pause.Reached.Wait(TimeSpan.FromSeconds(30)), Is.True, "Checkpoint did not reach WAIT_FLUSH");

                using (var session = writeStore.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, SharedListFunctions>(new SharedListFunctions()))
                {
                    TestObjectInput input = new() { value = RmwValue };
                    TestObjectOutput output = new();
                    var status = session.BasicContext.RMW(key, ref input, ref output);
                    if (status.IsPending)
                        _ = session.BasicContext.CompletePending(wait: true);
                }
                newRecordTail = writeStore.Log.TailAddress;

                // PostCopyUpdater appended to the list that (v) and (v+1) share, so (v)'s live object now carries
                // (v+1) content. Only the preserved image still holds the pre-mutation bytes.
                Assert.That(shared, Has.Count.EqualTo(InitialCount + 1), "The RMW did not take the CopyUpdate path");
                Assert.That(newRecordTail, Is.GreaterThan(sourceTail), "The RCU did not allocate a new record");

                // Distinguishes "the (v) image was never preserved" from "it was preserved but not used": the cache
                // serializes the source object, so a count of zero means CheckpointNeedsSourceImage said no.
                Assert.That(sourceObject.DoSerializeCount, Is.GreaterThanOrEqualTo(1),
                    $"{checkpointType}: the (v) object was never serialized during the RMW, so its pre-mutation image was "
                    + "never preserved -- CheckpointNeedsSourceImage returned false for this record");
                int[] countsAtRmw;
                lock (sourceObject.SerializedItemCounts)
                    countsAtRmw = [.. sourceObject.SerializedItemCounts];
                Assert.That(countsAtRmw[0], Is.EqualTo(InitialCount),
                    $"{checkpointType}: the preserved (v) image already held {countsAtRmw[0]} items, so it was captured "
                    + "after the successor mutated the shared collection rather than before");

                pause.Release.Set();
                await writeStore.CompleteCheckpointAsync().ConfigureAwait(false);

                lock (sourceObject.SerializedItemCounts)
                {
                    serializedCounts = [.. sourceObject.SerializedItemCounts];
                    serializedStacks = [.. sourceObject.SerializedStacks];
                }
            }
            finally
            {
                writeStore.Dispose();
            }

            var recoverStore = CreateStore(ckptLog, ckptObjLog, dir);
            try
            {
                _ = await recoverStore.RecoverAsync().ConfigureAwait(false);

                var functions = new SharedListFunctions();
                using var session = recoverStore.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, SharedListFunctions>(functions);
                TestObjectInput input = new();
                TestObjectOutput output = new();
                var status = session.BasicContext.Read(key, ref input, ref output);
                if (status.IsPending)
                    _ = session.BasicContext.CompletePending(wait: true);

                var recovered = functions.LastRead;
                Assert.That(recovered, Is.Not.Null, $"The key did not survive {checkpointType} recovery");

                // The (v) record lies below the tail sampled before the RCU; the (v+1) record at or above it.
                var recoveredIsSourceVersion = functions.LastReadAddress < sourceTail;
                var expected = recoveredIsSourceVersion ? InitialCount : InitialCount + 1;

                Assert.That(recovered.Items, Has.Count.EqualTo(expected),
                    $"{checkpointType} recovered the {(recoveredIsSourceVersion ? "(v)" : "(v+1)")} record at address "
                    + $"{functions.LastReadAddress} (v below {sourceTail}, v+1 at or above it) carrying "
                    + $"{recovered.Items.Count} items, but that version's content is {expected} items. "
                    + $"(v) was serialized {serializedCounts.Length} time(s) with item counts [{string.Join(", ", serializedCounts)}]: "
                    + "a single 8 means the preserved image was correct and something else wrote the record, while a "
                    + "trailing 9 means the live, already-mutated object was serialized again after the image was taken."
                    + $"{Environment.NewLine}Callers:{Environment.NewLine}{string.Join(Environment.NewLine + "----" + Environment.NewLine, serializedStacks)}");

                if (recoveredIsSourceVersion)
                    Assert.That(recovered.Items, Does.Not.Contain(RmwValue),
                        "The (v) image must not contain the value appended by the (v+1) PostCopyUpdater");
            }
            finally
            {
                recoverStore.Dispose();
            }
        }

        /// <summary>
        /// A Snapshot checkpoint recovers from the main log every page below the main log's <c>FlushedUntilAddress</c> as
        /// sampled at <see cref="Phase.PERSISTENCE_CALLBACK"/>, and overlays the snapshot file only above it. So a page that
        /// Snapshot has written and that the ReadOnly flush then rewrites before that sample is recovered from the ReadOnly
        /// image, and that image must still resolve to the <c>(v)</c> content.
        /// </summary>
        /// <remarks>
        /// Snapshot is held on a record of the following page, so the source's page is complete while coordination is still
        /// installed. The RMW runs in that window, while the source is still in the mutable region and so not frozen for
        /// flush. <paramref name="readOnlyFlushesThePageFirst"/> selects whether the ReadOnly flush then writes the source's
        /// page before the checkpoint samples the main log's FlushedUntilAddress. When it does not, recovery reads the page
        /// from the snapshot file; that case is the control.
        /// </remarks>
        [Test]
        [Category("TsavoriteKV")]
        [Category("CheckpointRestore")]
        public async Task SnapshotPageRewrittenByReadOnlyKeepsSourceImage([Values] bool readOnlyFlushesThePageFirst)
        {
            if (!ObjectFlushInjection.IsAvailable)
                Assert.Ignore("ObjectFlushInjection is compiled out in Release, so the Snapshot flush cannot be held on a later page.");

            const int InitialCount = 8;
            const int RmwValue = 99;

            var dir = Path.Join(MethodTestDir, "SnapshotThenReadOnly");
            _ = Directory.CreateDirectory(dir);
            var key = new TestObjectKey { key = 1 };

            using var ckptLog = Devices.CreateLogDevice(Path.Join(dir, "Ckpt.log"), deleteOnClose: false);
            using var ckptObjLog = Devices.CreateLogDevice(Path.Join(dir, "Ckpt.obj.log"), deleteOnClose: false);

            static TsavoriteKV<ClassStoreFunctions, ClassAllocator> CreateStore(IDevice log, IDevice objlog, string dir)
                => new(new()
                {
                    IndexSize = 1L << 13,
                    LogDevice = log,
                    ObjectLogDevice = objlog,
                    MutableFraction = 0.9,
                    LogMemorySize = 1L << 16,
                    PageSize = 1L << 13,
                    CheckpointDir = dir
                }, StoreFunctions.Create(new TestObjectKey.Comparer(), () => new SharedListSerializer())
                 , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));

            var shared = new List<int>();
            for (var ii = 1; ii <= InitialCount; ++ii)
                shared.Add(ii);

            using var snapshotHeld = new ManualResetEventSlim(false);
            using var releaseSnapshot = new ManualResetEventSlim(false);

            long sourceAddress, sourceTail;
            var writeStore = CreateStore(ckptLog, ckptObjLog, dir);
            try
            {
                long holdAddress;
                using (var session = writeStore.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, SharedListFunctions>(new SharedListFunctions()))
                {
                    sourceAddress = writeStore.Log.TailAddress;
                    _ = session.BasicContext.Upsert(key, new SharedListHeapObject(shared), Empty.Default);
                    sourceTail = writeStore.Log.TailAddress;

                    // Fill the source's page, then put one object record on the next page for Snapshot to be held on.
                    var nextKey = 2;
                    while (writeStore.hlogBase.GetPage(writeStore.Log.TailAddress) == writeStore.hlogBase.GetPage(sourceAddress))
                        _ = session.BasicContext.Upsert(new TestObjectKey { key = nextKey++ }, new SharedListHeapObject([nextKey]), Empty.Default);
                    holdAddress = writeStore.Log.TailAddress;
                    _ = session.BasicContext.Upsert(new TestObjectKey { key = nextKey }, new SharedListHeapObject([nextKey]), Empty.Default);
                }

                var sourcePageEnd = writeStore.hlogBase.GetLogicalAddressOfStartOfPage(writeStore.hlogBase.GetPage(sourceAddress) + 1);
                Assert.That(writeStore.hlogBase.GetPage(writeStore.Log.TailAddress), Is.EqualTo(writeStore.hlogBase.GetPage(sourceAddress) + 1));
                Assert.That(writeStore.Log.ReadOnlyAddress, Is.LessThanOrEqualTo(sourceAddress), "The source must start in the mutable region");

                // Only the Snapshot flush reaches the next page: the ReadOnly flush below stops at the end of the source's page.
                var holdPage = writeStore.hlogBase.GetPage(holdAddress);
                ObjectFlushInjection.Hook = (phase, logicalAddress) =>
                {
                    if (phase != ObjectFlushPhase.AfterRecordWritten || writeStore.hlogBase.GetPage(logicalAddress) != holdPage || snapshotHeld.IsSet)
                        return;
                    snapshotHeld.Set();
                    _ = releaseSnapshot.Wait(TimeSpan.FromSeconds(30));
                };

                Assert.That(writeStore.TryInitiateFullCheckpoint(out _, CheckpointType.Snapshot), Is.True);
                Assert.That(snapshotHeld.Wait(TimeSpan.FromSeconds(30)), Is.True, "Snapshot never reached the record on the next page");

                // The source's page write was issued before the held page; wait for its completion to advance the limit.
                var coordination = writeStore._hybridLogCheckpoint.snapshotFlushCoordination;
                Assert.That(coordination, Is.Not.Null, "Snapshot coordination was not created");
                var sourcePage = writeStore.hlogBase.GetPage(sourceAddress);
                var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(30);
                while (coordination.ReadOnlyFlushPageLimit <= sourcePage && DateTime.UtcNow < deadline)
                    Thread.Sleep(1);
                Assert.That(coordination.ReadOnlyFlushPageLimit, Is.GreaterThan(sourcePage), "Snapshot did not complete the source's page");
                Assert.That(writeStore.hlogBase.HasSnapshotFlushCoordination, Is.True, "Snapshot finished instead of being held");

                using (var session = writeStore.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, SharedListFunctions>(new SharedListFunctions()))
                {
                    TestObjectInput input = new() { value = RmwValue };
                    TestObjectOutput output = new();
                    var status = session.BasicContext.RMW(key, ref input, ref output);
                    if (status.IsPending)
                        _ = session.BasicContext.CompletePending(wait: true);
                }
                Assert.That(shared, Has.Count.EqualTo(InitialCount + 1), "The RMW did not take the CopyUpdate path");

                if (readOnlyFlushesThePageFirst)
                {
                    writeStore.Log.ShiftReadOnlyAddress(sourcePageEnd, wait: true);
                    Assert.That(writeStore.Log.FlushedUntilAddress, Is.GreaterThanOrEqualTo(sourcePageEnd), "The ReadOnly flush did not write the source's page");
                }

                releaseSnapshot.Set();
                await writeStore.CompleteCheckpointAsync().ConfigureAwait(false);
            }
            finally
            {
                releaseSnapshot.Set();
                ObjectFlushInjection.Reset();
                writeStore.Dispose();
            }

            var recoverStore = CreateStore(ckptLog, ckptObjLog, dir);
            try
            {
                _ = await recoverStore.RecoverAsync().ConfigureAwait(false);

                var functions = new SharedListFunctions();
                using var session = recoverStore.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, SharedListFunctions>(functions);
                TestObjectInput input = new();
                TestObjectOutput output = new();
                var status = session.BasicContext.Read(key, ref input, ref output);
                if (status.IsPending)
                    _ = session.BasicContext.CompletePending(wait: true);

                Assert.That(status.Found, Is.True, "The key did not survive recovery");
                Assert.That(functions.LastReadAddress, Is.LessThan(sourceTail), "Recovery should resolve the key to the (v) record");
                Assert.That(functions.LastRead, Is.Not.Null,
                    $"The recovered (v) record at {functions.LastReadAddress} no longer holds an object value "
                    + $"(readOnlyFlushesThePageFirst: {readOnlyFlushesThePageFirst})");
                Assert.That(functions.LastRead.Items, Has.Count.EqualTo(InitialCount),
                    $"The recovered (v) record carries {functions.LastRead.Items.Count} items rather than its own {InitialCount}");
            }
            finally
            {
                recoverStore.Dispose();
            }
        }

        /// <summary>
        /// A ReadOnly flush issued before a checkpoint starts can still be walking its page when the checkpoint is in
        /// <see cref="Phase.IN_PROGRESS"/>, so a (v+1) RMW can supersede a record on that page before or after the flush reaches it.
        /// Snapshot coordination does not prevent this: it orders the ReadOnly flush against Snapshot's own page writes, and becomes
        /// restrictive only on entry to <see cref="Phase.WAIT_FLUSH"/>, where Snapshot first waits for this flush to finish. FoldOver
        /// has no coordination. The flush walks with the epoch suspended, so the state machine advances meanwhile.
        /// </summary>
        /// <remarks>
        /// The page is recovered from this flush's image, so the image must resolve to the record's (v) content either way. The
        /// serialization claim orders the two: if the flush serializes the record first, it writes the unedited value and the RMW's
        /// preserved image is redundant. If the RMW goes first, a FoldOver flush writes the preserved image instead of the live
        /// value, and a Snapshot flush writes a null value that recovery repoints at the RMW's capture.
        /// </remarks>
        [Test]
        [Category("TsavoriteKV")]
        [Category("CheckpointRestore")]
        public async Task RecordSupersededDuringAPreCheckpointReadOnlyFlushKeepsItsImage(
            [Values(CheckpointType.FoldOver, CheckpointType.Snapshot)] CheckpointType checkpointType,
            [Values] bool rmwBeforeTheFlushReachesTheRecord)
        {
            if (!ObjectFlushInjection.IsAvailable)
                Assert.Ignore("ObjectFlushInjection is compiled out in Release, so the ReadOnly flush cannot be held mid-page.");

            const int InitialCount = 8;
            const int RmwValue = 99;

            var dir = Path.Join(MethodTestDir, $"{checkpointType}-{rmwBeforeTheFlushReachesTheRecord}");
            _ = Directory.CreateDirectory(dir);
            var earlierKey = new TestObjectKey { key = 1 };
            var key = new TestObjectKey { key = 2 };

            using var ckptLog = Devices.CreateLogDevice(Path.Join(dir, "Ckpt.log"), deleteOnClose: false);
            using var ckptObjLog = Devices.CreateLogDevice(Path.Join(dir, "Ckpt.obj.log"), deleteOnClose: false);

            static TsavoriteKV<ClassStoreFunctions, ClassAllocator> CreateStore(IDevice log, IDevice objlog, string dir)
                => new(new()
                {
                    IndexSize = 1L << 13,
                    LogDevice = log,
                    ObjectLogDevice = objlog,
                    MutableFraction = 0.9,
                    LogMemorySize = 1L << 16,
                    PageSize = 1L << 13,
                    CheckpointDir = dir
                }, StoreFunctions.Create(new TestObjectKey.Comparer(), () => new SharedListSerializer())
                 , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));

            var shared = new List<int>();
            for (var ii = 1; ii <= InitialCount; ++ii)
                shared.Add(ii);

            using var flushHeld = new ManualResetEventSlim(false);
            using var releaseFlush = new ManualResetEventSlim(false);
            var pause = new PauseAtPhase(Phase.WAIT_INDEX_CHECKPOINT);

            long sourceTail;
            int[] serializedCounts;
            var sourceObject = new SharedListHeapObject(shared);
            var writeStore = CreateStore(ckptLog, ckptObjLog, dir);
            try
            {
                long earlierAddress, sourceAddress;
                using (var session = writeStore.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, SharedListFunctions>(new SharedListFunctions()))
                {
                    earlierAddress = writeStore.Log.TailAddress;
                    _ = session.BasicContext.Upsert(earlierKey, new SharedListHeapObject([0]), Empty.Default);
                    sourceAddress = writeStore.Log.TailAddress;
                    _ = session.BasicContext.Upsert(key, sourceObject, Empty.Default);
                    sourceTail = writeStore.Log.TailAddress;
                }
                Assert.That(writeStore.hlogBase.GetPage(earlierAddress), Is.EqualTo(writeStore.hlogBase.GetPage(sourceAddress)),
                    "Both records must be on one page");

                // Hold the ReadOnly flush before it reaches the source, or just after it has serialized the source.
                var (holdPhase, holdAddress) = rmwBeforeTheFlushReachesTheRecord
                    ? (ObjectFlushPhase.AfterCapture, earlierAddress)
                    : (ObjectFlushPhase.AfterRecordWritten, sourceAddress);
                ObjectFlushInjection.Hook = (phase, logicalAddress) =>
                {
                    if (phase != holdPhase || logicalAddress != holdAddress || flushHeld.IsSet)
                        return;
                    flushHeld.Set();
                    _ = releaseFlush.Wait(TimeSpan.FromSeconds(30));
                };

                // No checkpoint is active when this flush is issued.
                var readOnlyShift = Task.Run(() => writeStore.Log.ShiftReadOnlyAddress(sourceTail, wait: true));
                Assert.That(flushHeld.Wait(TimeSpan.FromSeconds(30)), Is.True, "The ReadOnly flush never reached the hold point");

                // The pause runs after IN_PROGRESS is published, so (v+1) operations are running.
                writeStore.stateMachineDriver.UnsafeRegisterCallback(pause);
                Assert.That(writeStore.TryInitiateFullCheckpoint(out _, checkpointType), Is.True);
                Assert.That(pause.Reached.Wait(TimeSpan.FromSeconds(30)), Is.True,
                    "The checkpoint did not reach IN_PROGRESS while the ReadOnly flush was walking the source's page");
                Assert.That(writeStore.hlog.IsFrozenForFlush(sourceAddress), Is.True, "The source's ReadOnly flush must still be in flight");

                long newRecordAddress;
                using (var session = writeStore.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, SharedListFunctions>(new SharedListFunctions()))
                {
                    newRecordAddress = writeStore.Log.TailAddress;
                    TestObjectInput input = new() { value = RmwValue };
                    TestObjectOutput output = new();
                    var status = session.BasicContext.RMW(key, ref input, ref output);
                    if (status.IsPending)
                        _ = session.BasicContext.CompletePending(wait: true);
                }
                Assert.That(shared, Has.Count.EqualTo(InitialCount + 1), "The RMW did not take the CopyUpdate path");
                Assert.That(writeStore.hlogBase._wrapper.CreateLogRecord(newRecordAddress).Info.IsInNewVersion, Is.True,
                    "The RMW was not a (v+1) operation");

                releaseFlush.Set();
                pause.Release.Set();
                await writeStore.CompleteCheckpointAsync().ConfigureAwait(false);
                Assert.That(readOnlyShift.Wait(TimeSpan.FromSeconds(30)), Is.True, "The ReadOnly shift did not complete");

                lock (sourceObject.SerializedItemCounts)
                    serializedCounts = [.. sourceObject.SerializedItemCounts];
            }
            finally
            {
                releaseFlush.Set();
                pause.Release.Set();
                ObjectFlushInjection.Reset();
                writeStore.Dispose();
            }

            var recoverStore = CreateStore(ckptLog, ckptObjLog, dir);
            try
            {
                _ = await recoverStore.RecoverAsync().ConfigureAwait(false);

                var functions = new SharedListFunctions();
                using var session = recoverStore.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, SharedListFunctions>(functions);
                TestObjectInput input = new();
                TestObjectOutput output = new();
                var status = session.BasicContext.Read(key, ref input, ref output);
                if (status.IsPending)
                    _ = session.BasicContext.CompletePending(wait: true);

                Assert.That(functions.LastRead, Is.Not.Null, $"{checkpointType} recovery lost the (v) record's object");
                Assert.That(functions.LastReadAddress, Is.LessThan(sourceTail), "Recovery should resolve the key to the (v) record");
                Assert.That(functions.LastRead.Items, Is.EqualTo(shared.GetRange(0, InitialCount)),
                    $"{checkpointType} recovered the (v) record with content other than its own");
            }
            finally
            {
                recoverStore.Dispose();
            }

            // Which serializations run depends on the order: if the flush serialized the (v) object first, the RMW's preserved
            // image is redundant; if the RMW went first, the flush wrote that image rather than serializing the live object.
            Assert.That(serializedCounts, Is.Not.Empty, "The (v) object was never serialized");
            Assert.That(serializedCounts, Has.All.EqualTo(InitialCount),
                $"A serialization of the (v) object saw edited content; item counts: [{string.Join(", ", serializedCounts)}]");
        }

        /// <summary>
        /// A Delete that supersedes a (v) record while the checkpoint is still capturing it must not dispose the source until no flush
        /// reads it; it then disposes it exactly once.
        /// <para>
        /// CPR and the epoch bumps in <c>RunStateMachine</c> make the *version* decision correct — the Delete below
        /// correctly RCUs a (v+1) tombstone over a (v) source — but neither waits for the snapshot's data capture, and
        /// the session's <c>IsInV1</c> stays true through <see cref="Phase.WAIT_FLUSH"/> for exactly that reason.
        /// Disposing here would run
        /// <c>LogField.ClearObjectIdAndConvertToInline</c> on the live record: it calls <c>objectIdMap.Free</c> and flips
        /// the field to inline, while the snapshot flush, which writes the live page, resolves the record's object id against
        /// the live map — so a later record reusing that slot gets serialized in its place.
        /// </para>
        /// <para>
        /// The Delete marks the source instead, and the drain disposes it once FlushedUntilAddress passes it. On this real
        /// main-log device that is safe: the Snapshot flush completes a page before the page's ReadOnly flush can. Leaving the
        /// source to eviction without a mark would never run the delete trigger, because eviction calls only OnEvict.
        /// </para>
        /// <para>
        /// This is a different hazard from the one #2101 fixed: <c>CacheSerializedObjectData</c> preserves the (v)
        /// *content* inside the object, and says nothing about the *slot*. It is also a path that test does not reach,
        /// because the RMW CopyUpdate path caches rather than disposing as deleted; Delete and the
        /// expired-source RMW paths are the callers that do.
        /// </para>
        /// </summary>
        [Test]
        [Category("TsavoriteKV")]
        [Category("CheckpointRestore")]
        public async Task DeleteDuringCheckpointDisposesTheFrozenSourceOnlyOnceItsFlushCompletes()
        {
            var key = new TestObjectKey { key = 1 };
            var sourceAddress = store.Log.TailAddress;

            using (var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, SharedListFunctions>(new SharedListFunctions()))
                _ = session.BasicContext.Upsert(key, new SharedListHeapObject([1, 2, 3]), Empty.Default);

            // On an empty log the tail reads below the first page's first valid address, so the record actually lands
            // after the page header rather than at the address sampled above.
            var firstValidAddress = store.hlogBase.GetFirstValidLogicalAddressOnPage(0);
            if (sourceAddress < firstValidAddress)
                sourceAddress = firstValidAddress;

            var sourceBeforeCheckpoint = store.hlogBase._wrapper.CreateLogRecord(sourceAddress);
            Assert.That(sourceBeforeCheckpoint.DataHeader.ValueIsInline, Is.False,
                "the source record should hold an object id, otherwise this test cannot observe the slot being freed");

            // Create the session BEFORE the checkpoint so it participates in the state machine from PREPARE onward, which
            // is how a real long-lived session behaves. IsFrozen reads the *session's* phase, and a session created in the
            // middle of the window may not have adopted it yet.
            using var deleteSession = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, SharedListFunctions>(new SharedListFunctions());

            // Pause on entry to WAIT_FLUSH: IN_PROGRESS has been published and the fuzzy region has opened, so the
            // Delete below is (v+1) against a (v) source that the snapshot has not finished capturing.
            var pause = new PauseAtPhase(Phase.WAIT_FLUSH);
            store.stateMachineDriver.UnsafeRegisterCallback(pause);

            Assert.That(store.TryInitiateFullCheckpoint(out _, CheckpointType.Snapshot), Is.True);
            Assert.That(pause.Reached.Wait(TimeSpan.FromSeconds(30)), Is.True, "Checkpoint did not reach WAIT_FLUSH");

            var tailBeforeDelete = store.Log.TailAddress;
            Assert.That(store.SystemState.Phase, Is.AnyOf(Phase.IN_PROGRESS, Phase.WAIT_INDEX_CHECKPOINT),
                "precondition: the store must still be capturing the (v) image, or IsFrozen cannot engage");

            var status = deleteSession.BasicContext.Delete(key, Empty.Default);
            if (status.IsPending)
                _ = deleteSession.BasicContext.CompletePending(wait: true);

            var source = store.hlogBase._wrapper.CreateLogRecord(sourceAddress);
            Assert.Multiple(() =>
            {
                Assert.That(store.Log.TailAddress, Is.GreaterThan(tailBeforeDelete),
                    "the Delete did not RCU a new tombstone, so it never reached the superseded-source path");
                Assert.That(source.DataHeader.ValueIsInline, Is.False,
                    "the checkpoint-frozen source was disposed: its ObjectIdMap slot was freed and the field converted to inline, "
                    + "which lets the in-flight snapshot resolve that id to whatever record reuses the slot");
                Assert.That(source.Info.DeferredDispose, Is.True,
                    "a checkpoint freeze must be marked, so that the drain or eviction disposes it once no flush reads it; "
                    + $"the record's info is {source.Info}");
            });

            pause.Release.Set();
            await store.CompleteCheckpointAsync().ConfigureAwait(false);

            // The ReadOnly flush of the source's page lets the drain dispose it while the page is still resident. The drain runs after the
            // flush completion releases this wait, so poll for it.
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
            Assert.That(SpinWait.SpinUntil(() => store.hlogBase._wrapper.CreateLogRecord(sourceAddress).DataHeader.ValueIsInline, TimeSpan.FromSeconds(30)),
                Is.True, "the drain did not dispose the deleted source once its flush completed");
            Assert.That(store.Log.HeadAddress, Is.LessThanOrEqualTo(sourceAddress), "the source was evicted, so eviction rather than the drain may have disposed it");
            Assert.That(store.hlogBase._wrapper.CreateLogRecord(sourceAddress).Info.DeferredDispose, Is.False, "the drain claims the mark when it disposes the source");
        }

        internal class SharedListFunctions : SessionFunctionsBase<TestObjectInput, TestObjectOutput, Empty>
        {
            /// <summary>Set by <see cref="Reader"/> so a test can inspect the recovered heap object, which
            /// <see cref="TestObjectOutput"/> cannot carry, and the address it came from.</summary>
            internal SharedListHeapObject LastRead;
            internal long LastReadAddress;

            public override bool Reader<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref TestObjectInput input, ref TestObjectOutput output, ref ReadInfo readInfo)
            {
                LastRead = srcLogRecord.DataHeader.ValueIsObject ? srcLogRecord.ValueObject as SharedListHeapObject : null;
                LastReadAddress = readInfo.Address;
                return true;
            }

            public override bool InitialUpdater(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref TestObjectInput input, ref TestObjectOutput output, ref RMWInfo rmwInfo)
                => dstLogRecord.TrySetValueObject(new SharedListHeapObject([input.value]));

            public override bool InPlaceUpdater(ref LogRecord logRecord, ref TestObjectInput input, ref TestObjectOutput output, ref RMWInfo rmwInfo)
            {
                ((SharedListHeapObject)logRecord.ValueObject).Items.Add(input.value);
                return true;
            }

            public override bool CopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref TestObjectInput input, ref TestObjectOutput output, ref RMWInfo rmwInfo)
                => true;

            public override bool PostCopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref TestObjectInput input, ref TestObjectOutput output, ref RMWInfo rmwInfo)
            {
                ((SharedListHeapObject)dstLogRecord.ValueObject).Items.Add(input.value);
                return true;
            }

            public override RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref TestObjectInput input)
                => new() { KeySize = srcLogRecord.Key.Length, ValueSize = ObjectIdMap.ObjectIdSize, ValueIsObject = true };

            public override RecordFieldInfo GetRMWInitialFieldInfo<TKey>(TKey key, ref TestObjectInput input)
                => new() { KeySize = key.KeyBytes.Length, ValueSize = ObjectIdMap.ObjectIdSize, ValueIsObject = true };

            public override RecordFieldInfo GetUpsertFieldInfo<TKey>(TKey key, IHeapObject value, ref TestObjectInput input)
                => new() { KeySize = key.KeyBytes.Length, ValueSize = ObjectIdMap.ObjectIdSize, ValueIsObject = true };
        }
    }
}