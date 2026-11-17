// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;
using SharedListFunctions = Tsavorite.test.recovery.CheckpointSharedObjectMutationTests.SharedListFunctions;
using SharedListHeapObject = Tsavorite.test.recovery.CheckpointSharedObjectMutationTests.SharedListHeapObject;

namespace Tsavorite.test.recovery
{
    using ClassAllocator = ObjectAllocator<StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>>;
    using ClassStoreFunctions = StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>;
    using ListSession = ClientSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, CheckpointSharedObjectMutationTests.SharedListFunctions,
        StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>, ObjectAllocator<StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>>>;

    /// <summary>
    /// Checkpoints whose overlap region holds a marked image. A marked image is the main-log image of a record that a (v+1) CopyUpdate
    /// superseded during a Snapshot checkpoint before any flush wrote it; the record's (v) value is in that checkpoint's object log. A Snapshot
    /// checkpoint's overlap region is <c>[S, M)</c>: the main-log range a ReadOnly flush writes during WAIT_FLUSH, after WAIT_FLUSH samples the
    /// snapshot start <c>S</c> and before PERSISTENCE_CALLBACK samples the main-log recovery end <c>M</c>. Recovery reads that range from the
    /// main log, so it reads a marked image there, and takes the image's value from the checkpoint's own object log.
    /// </summary>
    internal partial class SnapshotReferenceTests
    {
        /// <summary>
        /// Recovers checkpoint A from the log as checkpoint B left it, as when B's checkpoint is lost and recovery falls back to A. Each checkpoint's
        /// overlap region holds a marked image among other object records, and B's overlap region lies above A's snapshot region. The recovery must
        /// return what A's first recovery (Restore-1) returned, record for record, in memory and after eviction.
        /// </summary>
        /// <param name="continueFromRestore">Add the data after A to the store Restore-1 recovered, as after a restart. Restore-1 repairs A's
        /// marked image: it copies the image's value into the main object log and rewrites the image, unmarked, to reference the copy. So the
        /// final recovery reads that copy, which lies above A's recorded object-log tail and below the objects written after Restore-1. Otherwise
        /// Restore-1 recovers a copy of A's files while the original store goes on, as when A is never recovered before B, so A's image is still
        /// marked in the log B leaves, and the final recovery reads its value from A's object log.</param>
        /// <param name="useIndex">Recover A with its index checkpoint, or without one, so that recovery replays the log from its start</param>
        /// <param name="overflowKeys">Whether keys are overflow keys, which recovery reads from an object log</param>
        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task EarlierOverlapCheckpointRecoversFromALaterLog([Values] bool continueFromRestore, [Values] bool useIndex, [Values] bool overflowKeys)
        {
            const int SourceKeyA = 1;
            const int SourceKeyB = 2;
            const int FirstKeyA = 100;
            const int FirstKeyB = 100_000;
            const int UpdatedKeyCountA = 10;
            const int UpdateValue = 7;

            var primary = Path.Join(dir, "primary");
            _ = Directory.CreateDirectory(primary);
            using var primaryLog = Devices.CreateLogDevice(Path.Join(primary, "Capture.log"), deleteOnClose: false);
            using var primaryObjects = Devices.CreateLogDevice(Path.Join(primary, "Capture.obj.log"), deleteOnClose: false);

            // Every key written so far, with the items the store being written holds for it. A recovered state is read over all of these keys,
            // so a key the recovered checkpoint does not hold must not be found.
            var expected = new Dictionary<int, List<int>>();
            Dictionary<int, List<int>> expectedA;
            Dictionary<int, KeyState> restore1;
            Guid tokenA, tokenB;
            long sourceA, sourceB;

            var store = CreateStore(overflowKeys, primaryLog, primaryObjects, primary);
            try
            {
                // A: more pages than memory holds, so ReadOnly flushes have run before A and its overlap region starts above the log's start.
                var nextKey = FirstKeyA;
                UpsertUntilPage(store, expected, ref nextKey, untilPage: 10);
                sourceA = UpsertSourceRecord(store, expected, SourceKeyA);
                CompleteSourcePage(store, expected, ref nextKey, sourceA);

                expectedA = CopyExpected(expected);
                tokenA = await TakeCheckpointWithMarkedImageInOverlap(store, sourceA, SourceKeyA).ConfigureAwait(false);
                expected[SourceKeyA].Add(RmwValue);
                var infoA = ReadCheckpointInfo(store, tokenA);
                AssertMarkedImageInOverlap(infoA, sourceA, "A");

                // Restore-1: recover A, either as the store the data after A goes to, or from a copy while the original store goes on.
                var indexTokenA = useIndex ? tokenA : default;
                if (continueFromRestore)
                {
                    store.Dispose();
                    store = CreateStore(overflowKeys, primaryLog, primaryObjects, primary);
                    _ = await store.RecoverAsync(indexTokenA, tokenA).ConfigureAwait(false);
                    restore1 = ReadKeys(store, expected.Keys);
                    expected = CopyExpected(expectedA);
                }
                else
                    restore1 = await RecoverCopy(primary, "restore-1", indexTokenA, tokenA, overflowKeys, expected.Keys).ConfigureAwait(false);
                AssertKeys(restore1, expectedA, "Restore-1");
                Assert.That(restore1[SourceKeyA].Address, Is.EqualTo(sourceA), "Restore-1 must resolve A's source key to its marked image");

                // More data: update A's source and some of A's other keys, then write far enough past A's snapshot region that the ReadOnly
                // flushes before B have passed it too.
                using (var session = NewListSession(store))
                {
                    RmwAppend(session, expected, SourceKeyA, UpdateValue);
                    for (var key = FirstKeyA; key < FirstKeyA + UpdatedKeyCountA; key++)
                        RmwAppend(session, expected, key, UpdateValue);
                }
                nextKey = FirstKeyB;
                UpsertUntilPage(store, expected, ref nextKey, untilPage: store.hlogBase.GetPage(infoA.recoveredTailAddress) + 10);
                sourceB = UpsertSourceRecord(store, expected, SourceKeyB);
                CompleteSourcePage(store, expected, ref nextKey, sourceB);

                tokenB = await TakeCheckpointWithMarkedImageInOverlap(store, sourceB, SourceKeyB).ConfigureAwait(false);
                expected[SourceKeyB].Add(RmwValue);
                var infoB = ReadCheckpointInfo(store, tokenB);
                AssertMarkedImageInOverlap(infoB, sourceB, "B");
                Assert.That(infoB.snapshotFileLogicalStartAddress, Is.GreaterThanOrEqualTo(infoA.recoveredTailAddress),
                    "B's overlap region must lie above A's snapshot region");
            }
            finally
            {
                store.Dispose();
            }

            // A, from the log as B left it: nothing has changed the log since B. B's records lie above A's tail, so recovering A must not find
            // them, and must find each of A's keys where Restore-1 did.
            store = CreateStore(overflowKeys, primaryLog, primaryObjects, primary);
            try
            {
                _ = await store.RecoverAsync(useIndex ? tokenA : default, tokenA).ConfigureAwait(false);
                AssertSameKeys(ReadKeys(store, expected.Keys), restore1, "A, recovered from the log B left");
                store.Log.FlushAndEvict(wait: true);
                AssertSameKeys(ReadKeys(store, expected.Keys), restore1, "A, recovered from the log B left, after eviction");
            }
            finally
            {
                store.Dispose();
            }
        }

        /// <summary>
        /// Take a full Snapshot checkpoint whose overlap region holds the source's marked image and the object records below it. The (v+1) RMW
        /// captures the source while the checkpoint is held on its way into WAIT_INDEX_CHECKPOINT, which is before WAIT_FLUSH samples S and starts
        /// the Checkpoint Flush. WAIT_FLUSH is then held open, after it has sampled S and before PERSISTENCE_CALLBACK samples M, while a ReadOnly
        /// flush writes the log through the end of the source's page.
        /// </summary>
        /// <param name="store">The store</param>
        /// <param name="sourceAddress">The source record's address, on a page that is complete and still mutable</param>
        /// <param name="sourceKey">The source's key</param>
        /// <returns>The checkpoint's token</returns>
        static async Task<Guid> TakeCheckpointWithMarkedImageInOverlap(TsavoriteKV<ClassStoreFunctions, ClassAllocator> store, long sourceAddress, int sourceKey)
        {
            var sourcePageEnd = store.hlogBase.GetLogicalAddressOfStartOfPage(store.hlogBase.GetPage(sourceAddress) + 1);
            Assert.That(store.Log.ReadOnlyAddress, Is.LessThanOrEqualTo(sourceAddress), "The source must be mutable when the checkpoint starts");
            Assert.That(store.Log.TailAddress, Is.GreaterThan(sourcePageEnd), "The source's page must be complete");

            var pause = new PauseAtPhase(Phase.WAIT_INDEX_CHECKPOINT);
            var hold = new HoldPhaseOpen(store.stateMachineDriver, Phase.WAIT_FLUSH);
            store.stateMachineDriver.UnsafeRegisterCallback(pause);
            store.stateMachineDriver.UnsafeRegisterCallback(hold);
            Guid token;
            try
            {
                Assert.That(store.TryInitiateFullCheckpoint(out token, CheckpointType.Snapshot), Is.True);
                Assert.That(pause.Reached.Wait(TimeSpan.FromSeconds(30)), Is.True, "The checkpoint did not reach IN_PROGRESS");
                Rmw(store, sourceAddress, expectCapture: true, key: sourceKey);
                pause.Release.Set();

                Assert.That(hold.Entered.Wait(TimeSpan.FromSeconds(30)), Is.True, "The checkpoint did not reach WAIT_FLUSH");
                store.Log.ShiftReadOnlyAddress(sourcePageEnd, wait: true);
                Assert.That(store.Log.FlushedUntilAddress, Is.GreaterThanOrEqualTo(sourcePageEnd), "The ReadOnly flush did not write the source's page");
            }
            finally
            {
                pause.Release.Set();
                hold.Open();
            }
            await store.CompleteCheckpointAsync().ConfigureAwait(false);
            return token;
        }

        /// <summary>Check that a checkpoint's overlap region holds its marked image above other records, and that a snapshot region lies above
        /// the overlap region.</summary>
        static void AssertMarkedImageInOverlap(in HybridLogRecoveryInfo info, long sourceAddress, string checkpoint)
        {
            var (start, end, referenceStart, tail, begin) = (info.snapshotFileLogicalStartAddress, info.mainLogRecoveryEndAddress,
                info.snapshotObjectLogStartAddress, info.recoveredTailAddress, info.beginAddress);
            Assert.Multiple(() =>
            {
                Assert.That(start, Is.GreaterThan(begin), $"{checkpoint}'s overlap region must start above the log's start");
                Assert.That(start, Is.LessThan(sourceAddress), $"{checkpoint}'s overlap region must hold records below its marked image");
                Assert.That(end, Is.GreaterThan(sourceAddress), $"{checkpoint}'s overlap region must hold its marked image");
                Assert.That(referenceStart, Is.EqualTo(sourceAddress), $"{checkpoint}'s range of marked images must start at its marked image");
                Assert.That(tail, Is.GreaterThan(end), $"{checkpoint} must have a snapshot region above its overlap region");
            });
        }

        /// <summary>Read a checkpoint's metadata.</summary>
        static HybridLogRecoveryInfo ReadCheckpointInfo(TsavoriteKV<ClassStoreFunctions, ClassAllocator> store, Guid token)
        {
            HybridLogCheckpointInfo checkpoint = default;
            try
            {
                checkpoint.Recover(token, store.checkpointManager);
                return checkpoint.info;
            }
            finally
            {
                checkpoint.Dispose();
            }
        }

        /// <summary>Recover a checkpoint from a copy of a store's files, and read the given keys from it.</summary>
        async Task<Dictionary<int, KeyState>> RecoverCopy(string source, string name, Guid indexToken, Guid token, bool overflowKeys, IEnumerable<int> keys)
        {
            var target = Path.Join(dir, name);
            CopyDirectory(source, target);
            using var copyLog = Devices.CreateLogDevice(Path.Join(target, "Capture.log"), deleteOnClose: false);
            using var copyObjects = Devices.CreateLogDevice(Path.Join(target, "Capture.obj.log"), deleteOnClose: false);
            using var copyStore = CreateStore(overflowKeys, copyLog, copyObjects, target);
            _ = await copyStore.RecoverAsync(indexToken, token).ConfigureAwait(false);
            return ReadKeys(copyStore, keys);
        }

        /// <summary>Copy a store's files, including device files that a running store still has open.</summary>
        static void CopyDirectory(string source, string target)
        {
            foreach (var file in Directory.GetFiles(source, "*", SearchOption.AllDirectories))
            {
                var targetFile = Path.Join(target, Path.GetRelativePath(source, file));
                _ = Directory.CreateDirectory(Path.GetDirectoryName(targetFile));
                using var input = new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
                using var output = new FileStream(targetFile, FileMode.CreateNew, FileAccess.Write);
                input.CopyTo(output);
            }
        }

        static ListSession NewListSession(TsavoriteKV<ClassStoreFunctions, ClassAllocator> store)
            => store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, SharedListFunctions>(new SharedListFunctions());

        /// <summary>Upsert a new key whose list holds the key itself.</summary>
        static void Upsert(ListSession session, Dictionary<int, List<int>> expected, int key)
        {
            _ = session.BasicContext.Upsert(new TestObjectKey { key = key }, new SharedListHeapObject([key]), Empty.Default);
            expected[key] = [key];
        }

        /// <summary>Upsert a source record whose list holds <see cref="InitialCount"/> items, and return its address. The tail before the
        /// upsert is not always that address: a tail at a page's start precedes the page's header, which the record follows.</summary>
        static long UpsertSourceRecord(TsavoriteKV<ClassStoreFunctions, ClassAllocator> store, Dictionary<int, List<int>> expected, int key)
        {
            using var session = NewListSession(store);
            List<int> items = [.. Enumerable.Range(1, InitialCount)];
            TestObjectInput input = new();
            TestObjectOutput output = new();
            UpsertOptions upsertOptions = default;
            var status = session.BasicContext.Upsert(new TestObjectKey { key = key }, ref input, new SharedListHeapObject(items), ref output,
                ref upsertOptions, out var recordMetadata);
            Assert.That(status.Record.Created, Is.True, $"The source of key {key} must be a new record");
            expected[key] = [.. items];
            return recordMetadata.Address;
        }

        /// <summary>Append a value to a key's list.</summary>
        static void RmwAppend(ListSession session, Dictionary<int, List<int>> expected, int key, int value)
        {
            TestObjectInput input = new() { value = value };
            TestObjectOutput output = new();
            var status = session.BasicContext.RMW(new TestObjectKey { key = key }, ref input, ref output);
            if (status.IsPending)
                _ = session.BasicContext.CompletePending(wait: true);
            expected[key].Add(value);
        }

        static void UpsertUntilPage(TsavoriteKV<ClassStoreFunctions, ClassAllocator> store, Dictionary<int, List<int>> expected, ref int nextKey, long untilPage)
        {
            using var session = NewListSession(store);
            while (store.hlogBase.GetPage(store.Log.TailAddress) < untilPage)
                Upsert(session, expected, nextKey++);
        }

        /// <summary>Fill the source's page, then put one record on the next page, so a ReadOnly flush through the page's end writes all of it.</summary>
        static void CompleteSourcePage(TsavoriteKV<ClassStoreFunctions, ClassAllocator> store, Dictionary<int, List<int>> expected, ref int nextKey, long sourceAddress)
        {
            using var session = NewListSession(store);
            while (store.hlogBase.GetPage(store.Log.TailAddress) == store.hlogBase.GetPage(sourceAddress))
                Upsert(session, expected, nextKey++);
            Upsert(session, expected, nextKey++);
        }

        static Dictionary<int, List<int>> CopyExpected(Dictionary<int, List<int>> expected)
            => expected.ToDictionary(entry => entry.Key, entry => new List<int>(entry.Value));

        /// <summary>What a read of a key returns: whether it found the key, and if so the address of the record it read and that record's items.</summary>
        readonly record struct KeyState(bool Found, long Address, string Items)
        {
            public override string ToString() => Found ? $"[{Items}] at {Address}" : "not found";
        }

        static Dictionary<int, KeyState> ReadKeys(TsavoriteKV<ClassStoreFunctions, ClassAllocator> store, IEnumerable<int> keys)
        {
            var functions = new SharedListFunctions();
            using var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, SharedListFunctions>(functions);
            var state = new Dictionary<int, KeyState>();
            foreach (var key in keys)
            {
                functions.LastRead = null;
                functions.LastReadAddress = 0;
                TestObjectInput input = new();
                TestObjectOutput output = new();
                var status = session.BasicContext.Read(new TestObjectKey { key = key }, ref input, ref output);
                if (status.IsPending)
                {
                    Assert.That(session.BasicContext.CompletePendingWithOutputs(out var outputs, wait: true), Is.True);
                    (status, _) = GetSinglePendingResult(outputs);
                }
                state[key] = !status.Found ? default
                    : new KeyState(Found: true, functions.LastReadAddress, functions.LastRead is null ? "no object" : string.Join(", ", functions.LastRead.Items));
            }
            return state;
        }

        /// <summary>Check each key read against the items expected for it, or that it was not found if none are.</summary>
        static void AssertKeys(Dictionary<int, KeyState> state, Dictionary<int, List<int>> expected, string recovered)
        {
            var mismatches = new List<string>();
            foreach (var (key, actual) in state)
            {
                var wanted = expected.TryGetValue(key, out var items) ? $"[{string.Join(", ", items)}]" : "not found";
                var read = actual.Found ? $"[{actual.Items}]" : "not found";
                if (read != wanted)
                    mismatches.Add($"key {key}: {read}, expected {wanted}");
            }
            Assert.That(mismatches, Is.Empty, $"{recovered}: {mismatches.Count} keys read wrongly");
        }

        /// <summary>Check each key read against a reference state, address and items, or that it was not found if the reference has no entry.</summary>
        static void AssertSameKeys(Dictionary<int, KeyState> state, Dictionary<int, KeyState> reference, string recovered)
        {
            var mismatches = new List<string>();
            foreach (var (key, actual) in state)
            {
                var wanted = reference.TryGetValue(key, out var referenceState) ? referenceState : default;
                if (actual != wanted)
                    mismatches.Add($"key {key}: {actual}, expected {wanted}");
            }
            Assert.That(mismatches, Is.Empty, $"{recovered}: {mismatches.Count} keys differ from Restore-1");
        }

        /// <summary>
        /// Keeps the checkpoint state machine in a phase until <see cref="Open"/> is called, without blocking its driver. On the transition into
        /// the phase, which runs after the phase's own work has started, it adds a task to the work the driver waits for before it leaves the
        /// phase. It acts on the first such transition only.
        /// </summary>
        sealed class HoldPhaseOpen(StateMachineDriver driver, Phase phase) : IStateMachineCallback
        {
            /// <summary>The task the driver waits for; completing it lets the driver leave the phase.</summary>
            readonly TaskCompletionSource open = new(TaskCreationOptions.RunContinuationsAsynchronously);

            /// <summary>1 until the hold is placed, so that a later checkpoint passes through.</summary>
            int armed = 1;

            /// <summary>Set once the phase's work has started and the hold is in place.</summary>
            internal readonly ManualResetEventSlim Entered = new(false);

            public void BeforeEnteringState(SystemState next)
            {
                if (next.Phase != phase || Interlocked.Exchange(ref armed, 0) == 0)
                    return;

                // The type only labels the task in the driver's log if it faults, which this one does not.
                driver.AddToWaitingList(open.Task, StateMachineTaskType.SnapshotCheckpointSMTaskHybridLogFlushed);
                Entered.Set();
            }

            /// <summary>Let the driver leave the phase.</summary>
            internal void Open() => _ = open.TrySetResult();
        }
    }
}