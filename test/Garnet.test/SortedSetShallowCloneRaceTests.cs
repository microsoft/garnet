// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;

namespace Garnet.test
{
    using ObjectStoreAllocator = GenericAllocator<byte[], IGarnetObject, StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>>;
    using ObjectStoreFunctions = StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>;

    /// <summary>
    /// Demonstrates a race between Tsavorite's flush-path serialization and the
    /// CopyUpdater (RCU) path when an object's Clone() is shallow.
    ///
    /// A custom <see cref="PausableSerializeObject"/> pauses inside DoSerialize
    /// so we can deterministically trigger an RMW (CopyUpdater) on the same key
    /// while the flush thread is mid-serialization. Because Clone() shares the
    /// same mutable List, the CopyUpdater's post-copy mutation races with the
    /// flush thread's iteration of that List.
    /// </summary>
    [TestFixture]
    public class SortedSetShallowCloneRaceTests
    {
        TsavoriteKV<byte[], IGarnetObject, ObjectStoreFunctions, ObjectStoreAllocator> store;
        IDevice logDevice, objectLogDevice;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            CreateStore();
        }

        [TearDown]
        public void TearDown()
        {
            store?.Dispose();
            logDevice?.Dispose();
            objectLogDevice?.Dispose();
            logDevice = objectLogDevice = null;
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        /// <summary>
        /// End-to-end proof through the real Tsavorite store:
        ///
        ///   1. Upsert a PausableSerializeObject into the object store.
        ///   2. Dispose the session (so no thread blocks the epoch callback).
        ///   3. Kick off store.Log.Flush(true) on a background thread.
        ///      Flush shifts ReadOnlyAddress → epoch callback → WriteAsync →
        ///      shadow-copies the page, suspends epoch, then calls Serialize
        ///      on every value — including our object.
        ///   4. DoSerialize signals <c>serializeReached</c>, then waits on
        ///      <c>proceedWithSerialize</c>.  The flush thread is now parked
        ///      inside DoSerialize with the shared List about to be iterated.
        ///   5. On the main thread, open a new session and issue an RMW on
        ///      the same key.  The record is below ReadOnlyAddress so
        ///      InternalRMW takes the CopyUpdater path:
        ///        CopyUpdater  → oldValue.CopyUpdate(…, false)
        ///                       → Clone() — shallow, shares the List
        ///        PostCopyUpdater → mutates the clone's (== original's) List
        ///   6. Signal <c>proceedWithSerialize</c> so the flush thread resumes
        ///      its foreach over the now-concurrently-modified List.
        ///   7. Assert that the race was detected (InvalidOperationException
        ///      from the Dictionary/List enumerator, or a flag set by the
        ///      concurrent-access detector).
        /// </summary>
        [Test]
        [Repeat(5)]
        public void FlushSerializationRacesWithCopyUpdaterClone()
        {
            var shared = new SharedState(entryCount: 5000);
            var obj = new PausableSerializeObject(shared);
            var key = new byte[] { 0 };

            // ── 1. Upsert, then release the session so its epoch doesn't
            //       block the flush callback.
            using (var session = store.NewSession<IGarnetObject, IGarnetObject, Empty, CopyUpdateFunctions>(new CopyUpdateFunctions()))
            {
                session.BasicContext.Upsert(key, obj);
            }

            // ── 2. Flush on a background thread.
            //       Flush(true) enters ProtectAndDrain, fires the epoch
            //       callback → OnPagesMarkedReadOnly → AsyncFlushPages →
            //       WriteAsync → shadow copy → epoch.Suspend() → Serialize.
            //       Our DoSerialize pauses after signaling serializeReached.
            var flushTask = Task.Run(() =>
            {
                try { store.Log.Flush(true); }
                catch { /* serialization may throw — that's what we're proving */ }
            });

            // ── 3. Wait until the flush thread is parked inside DoSerialize.
            ClassicAssert.IsTrue(
                shared.SerializeReached.Wait(TimeSpan.FromSeconds(10)),
                "Flush thread never reached DoSerialize — check store/epoch setup.");

            // ── 4. RMW on a fresh session.  The record is below ReadOnlyAddress
            //       (Flush shifted it), so InternalRMW → CopyUpdater path.
            //       CopyUpdater: CopyUpdate → Clone (shallow, shares List)
            //       PostCopyUpdater: mutates the shared List.
            using (var session = store.NewSession<IGarnetObject, IGarnetObject, Empty, CopyUpdateFunctions>(new CopyUpdateFunctions()))
            {
                IGarnetObject input = obj;
                try { session.BasicContext.RMW(ref key, ref input); }
                catch { /* may throw if race corrupts something — fine */ }
            }

            // ── 5. The flush thread resumes its foreach (signaled by
            //       PostCopyUpdater) over the List that PostCopyUpdater
            //       is concurrently mutating.

            flushTask.Wait(TimeSpan.FromSeconds(15));

            // ── 6. Assert
            if (shared.RaceException != null)
            {
                Assert.Pass(
                    $"Race condition reproduced through Tsavorite flush + CopyUpdater. " +
                    $"DoSerialize threw: [{shared.RaceException.GetType().Name}] " +
                    $"{shared.RaceException.Message}");
            }

            ClassicAssert.IsTrue(shared.RaceDetected,
                "Expected the flush thread's DoSerialize to race with " +
                "CopyUpdater's clone mutation on the shared List.");
        }

        // ──────────────────────────────────────────────────────────────────────
        //  Infrastructure
        // ──────────────────────────────────────────────────────────────────────

        private void CreateStore()
        {
            logDevice ??= Devices.CreateLogDevice(TestUtils.MethodTestDir + "/hlog.log");
            objectLogDevice ??= Devices.CreateLogDevice(TestUtils.MethodTestDir + "/hlog.obj.log");

            var kvSettings = new KVSettings<byte[], IGarnetObject>
            {
                IndexSize = 1L << 13,
                LogDevice = logDevice,
                ObjectLogDevice = objectLogDevice,
                CheckpointDir = TestUtils.MethodTestDir
            };

            store = new(kvSettings,
                StoreFunctions<byte[], IGarnetObject>.Create(
                    new ByteArrayKeyComparer(),
                    () => new Tsavorite.core.ByteArrayBinaryObjectSerializer(),
                    () => new MyGarnetObjectSerializer()),
                (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));
        }

        // ──────────────────────────────────────────────────────────────────────
        //  Shared mutable state — held by original AND clone after Clone()
        // ──────────────────────────────────────────────────────────────────────

        internal sealed class SharedState
        {
            public readonly List<int> Data;
            public readonly ManualResetEventSlim SerializeReached = new(false);
            public readonly ManualResetEventSlim ProceedWithSerialize = new(false);
            public volatile bool RaceDetected;
            public volatile Exception RaceException;

            public SharedState(int entryCount)
            {
                Data = new List<int>(entryCount);
                for (int i = 0; i < entryCount; i++)
                    Data.Add(i);
            }
        }

        // ──────────────────────────────────────────────────────────────────────
        //  Custom IGarnetObject whose Clone() is deliberately shallow
        //  (mirrors SortedSetObject's copy-constructor sharing sortedSetDict)
        // ──────────────────────────────────────────────────────────────────────

        internal sealed class PausableSerializeObject : GarnetObjectBase
        {
            internal readonly SharedState Shared;

            public PausableSerializeObject(SharedState shared)
                : base(0, 64)
            {
                Shared = shared;
            }

            /// <summary>
            /// Shallow clone — shares the same <see cref="SharedState"/>
            /// (and therefore the same <c>Data</c> list) with the original.
            /// This is the same pattern as SortedSetObject's copy constructor.
            /// </summary>
            private PausableSerializeObject(PausableSerializeObject other)
                : base(other.Expiration, other.Size)
            {
                Shared = other.Shared;
            }

            public override byte Type => 0xFE;
            public override GarnetObjectBase Clone() => new PausableSerializeObject(this);
            public override void Dispose() { }

            public override void DoSerialize(BinaryWriter writer)
            {
                base.DoSerialize(writer);

                // ── Signal: "I am inside DoSerialize, about to iterate." ──
                Shared.SerializeReached.Set();

                // ── Pause: let the test thread trigger the RCU + mutation
                //    before we begin iterating the shared List.
                Shared.ProceedWithSerialize.Wait(TimeSpan.FromSeconds(10));

                writer.Write(Shared.Data.Count);
                try
                {
                    // This foreach races with PostCopyUpdater's Add() calls
                    // on the clone that shares the same Data list.
                    foreach (var item in Shared.Data)
                        writer.Write(item);
                }
                catch (InvalidOperationException ex)
                {
                    // "Collection was modified; enumeration operation may not execute."
                    Shared.RaceDetected = true;
                    Shared.RaceException = ex;
                }
            }

            public override bool Operate(ref ObjectInput input, ref GarnetObjectStoreOutput output, byte respProtocolVersion, out long sizeChange)
            {
                sizeChange = 0;
                return true;
            }

            public override unsafe void Scan(long start, out List<byte[]> items,
                out long cursor, int count = 10, byte* pattern = default,
                int patternLength = 0, bool isNoValue = false)
            {
                items = new List<byte[]>();
                cursor = 0;
            }
        }

        // ──────────────────────────────────────────────────────────────────────
        //  Session functions: CopyUpdater + PostCopyUpdater mirroring the real
        //  ObjectSessionFunctions flow (RMWMethods.cs:179-240)
        // ──────────────────────────────────────────────────────────────────────

        private sealed class CopyUpdateFunctions
            : SessionFunctionsBase<byte[], IGarnetObject, IGarnetObject, IGarnetObject, Empty>
        {
            /// <summary>
            /// Mirrors ObjectSessionFunctions.CopyUpdater (RMWMethods.cs:179):
            /// calls oldValue.CopyUpdate which does Clone() (shallow) and
            /// sets oldValue = null.
            /// </summary>
            public override bool CopyUpdater(ref byte[] key, ref IGarnetObject input,
                ref IGarnetObject oldValue, ref IGarnetObject newValue,
                ref IGarnetObject output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
            {
                //oldValue.CopyUpdate(ref oldValue, ref newValue, false);
                return true;
            }

            /// <summary>
            /// Mirrors ObjectSessionFunctions.PostCopyUpdater (RMWMethods.cs:240):
            /// value.Operate() — i.e. ZADD — mutates the clone's data structures.
            /// Because Clone() was shallow, this mutates the SAME List that the
            /// flush thread is iterating inside DoSerialize.
            ///
            /// We signal ProceedWithSerialize FIRST so the flush thread's foreach
            /// begins, then we mutate — both threads now touch the same List
            /// concurrently.
            /// </summary>
            public override bool PostCopyUpdater(ref byte[] key, ref IGarnetObject input,
                ref IGarnetObject oldValue, ref IGarnetObject value,
                ref IGarnetObject output, ref RMWInfo rmwInfo)
            {
                oldValue.CopyUpdate(ref oldValue, ref value, rmwInfo.RecordInfo.IsInNewVersion);
                var obj = (PausableSerializeObject)value;

                // Release the flush thread so it starts iterating the shared List
                obj.Shared.ProceedWithSerialize.Set();

                // Small yield to let the flush thread enter its foreach
                Thread.SpinWait(100);

                // Now mutate the shared list — races with DoSerialize's foreach
                for (int i = 0; i < 500; i++)
                    obj.Shared.Data.Add(i + 100_000);
                return true;
            }

            public override bool SingleReader(ref byte[] key, ref IGarnetObject input,
                ref IGarnetObject value, ref IGarnetObject dst, ref ReadInfo info)
            { dst = value; return true; }

            public override bool ConcurrentReader(ref byte[] key, ref IGarnetObject input,
                ref IGarnetObject value, ref IGarnetObject dst, ref ReadInfo info,
                ref RecordInfo recordInfo)
            { dst = value; return true; }
        }
    }
}
