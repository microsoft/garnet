// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Garnet.test;
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