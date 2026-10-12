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
                var count = reader.ReadInt32();
                var items = new List<int>(count);
                for (var i = 0; i < count; i++)
                    items.Add(reader.ReadInt32());
                obj = new SharedListHeapObject(items);
            }

            public override void Serialize(IHeapObject obj) => ((SharedListHeapObject)obj).DoSerialize(writer);
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
        /// A Delete that supersedes a (v) record while the checkpoint is still capturing it must NOT dispose the source.
        /// <para>
        /// CPR and the epoch bumps in <c>RunStateMachine</c> make the *version* decision correct — the Delete below
        /// correctly RCUs a (v+1) tombstone over a (v) source — but neither waits for the snapshot's data capture, and
        /// the session's <c>IsInV1</c> stays true through <see cref="Phase.WAIT_FLUSH"/> for exactly that reason.
        /// Disposing here would run
        /// <c>LogField.ClearObjectIdAndConvertToInline</c> on the live record: it calls <c>objectIdMap.Free</c> and flips
        /// the field to inline, while the snapshot holds a page *copy* that still carries the old object id and resolves
        /// ids against the *live* map — so a later record reusing that slot gets serialized in its place.
        /// </para>
        /// <para>
        /// This is a different hazard from the one #2101 fixed: <c>CacheSerializedObjectData</c> preserves the (v)
        /// *content* inside the object, and says nothing about the *slot*. It is also a path that test does not reach,
        /// because the RMW CopyUpdate path caches rather than calling <c>OnDisposeDeletedSource</c>; Delete and the
        /// expired-source RMW paths are the callers that do.
        /// </para>
        /// </summary>
        [Test]
        [Category("TsavoriteKV")]
        [Category("CheckpointRestore")]
        public async Task DeleteDuringCheckpointDoesNotDisposeTheFrozenSource()
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
                Assert.That(source.Info.DeferredDispose, Is.False,
                    "a checkpoint freeze must not be marked for the deferred-dispose drain; the drain releases on the main log's "
                    + "FlushedUntilAddress, which says nothing about whether the snapshot has captured the record");
            });

            pause.Release.Set();
            await store.CompleteCheckpointAsync().ConfigureAwait(false);
        }

        internal class SharedListFunctions : SessionFunctionsBase<TestObjectInput, TestObjectOutput, Empty>
        {
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