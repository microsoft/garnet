// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.recovery.objects
{
    using ClassAllocator = ObjectAllocator<StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>>;
    using ClassStoreFunctions = StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>;

    /// <summary>
    /// Recovery restores FlushedUntilAddress at the checkpoint's tail, which is usually mid-sector. That boundary must never be
    /// above the tail, or records written after recovery are treated as durable, never flushed, and lost on eviction. And the
    /// first flush after recovery, which starts in that sector and writes it whole, must keep the records below its start
    /// readable even though recovery's object load has since rewritten them in memory.
    /// </summary>
    [TestFixture]
    public class RecoveredFlushBoundaryTests : TestBase
    {
        sealed class PauseBeforePhase(Phase pauseAt) : IStateMachineCallback
        {
            internal readonly ManualResetEventSlim Reached = new(false);
            internal readonly ManualResetEventSlim Release = new(false);

            public void BeforeEnteringState(SystemState next)
            {
                if (next.Phase != pauseAt)
                    return;
                Reached.Set();
                _ = Release.Wait(TimeSpan.FromSeconds(60));
            }
        }

        sealed class LenientFunctions : TestObjectFunctions
        {
            public override void ReadCompletionCallback(ref DiskLogRecord srcLogRecord, ref TestObjectInput input, ref TestObjectOutput output, Empty ctx, Status status, RecordMetadata recordMetadata) { }
        }

        [SetUp]
        public void Setup() => RecreateDirectory(MethodTestDir);

        [TearDown]
        public void TearDown() => TestUtils.OnTearDown();

        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task ReadOnlyFlushPastCheckpointTailDoesNotRecoverAboveTail()
        {
            const int stableCount = 500;
            const int nextVersionCount = 2000;
            const int postRecoveryCount = 100;
            Guid token;

            Prepare(out var log, out var objlog, out var store);
            try
            {
                using var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, LenientFunctions>(new LenientFunctions());
                var bContext = session.BasicContext;
                for (var i = 0; i < stableCount; i++)
                    _ = bContext.Upsert(new TestObjectKey { key = i }, new TestObjectValue { value = i });

                // Callbacks run after the task hooks, so this holds the machine after the WAIT_FLUSH hook (tail sampled, Snapshot
                // issued) and before the PERSISTENCE_CALLBACK hook samples FlushedUntilAddress. IN_PROGRESS stays published.
                var pause = new PauseBeforePhase(Phase.WAIT_FLUSH);
                store.stateMachineDriver.UnsafeRegisterCallback(pause);
                ClassicAssert.IsTrue(store.TryInitiateHybridLogCheckpoint(out token, CheckpointType.Snapshot));
                Assert.That(pause.Reached.Wait(TimeSpan.FromSeconds(30)), Is.True, "never reached WAIT_FLUSH");

                // (v+1) writes move the tail well past the checkpoint's tail; flushing them moves FlushedUntilAddress past it too.
                try
                {
                    var checkpointTail = store._hybridLogCheckpoint.info.recoveredTailAddress;
                    for (var i = 0; i < nextVersionCount; i++)
                        _ = bContext.Upsert(new TestObjectKey { key = 10_000 + i }, new TestObjectValue { value = 10_000 + i });
                    store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
                    Assert.That(store.Log.FlushedUntilAddress, Is.GreaterThan(checkpointTail), "ReadOnly flush did not pass the checkpoint tail");
                }
                finally
                {
                    // Release the state machine even on failure, so disposing the store does not wait out the pause.
                    pause.Release.Set();
                }
                await store.CompleteCheckpointAsync().AsTask().ConfigureAwait(false);
            }
            finally
            {
                Destroy(log, objlog, store);
            }

            Prepare(out log, out objlog, out store);
            try
            {
                _ = await store.RecoverAsync(default, token).ConfigureAwait(false);
                Assert.That(store.Log.ReadOnlyAddress, Is.LessThanOrEqualTo(store.Log.TailAddress),
                    $"Recovered ReadOnlyAddress {store.Log.ReadOnlyAddress} (FlushedUntilAddress {store.Log.FlushedUntilAddress}) is above TailAddress {store.Log.TailAddress}");

                using var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, LenientFunctions>(new LenientFunctions());
                var bContext = session.BasicContext;
                for (var i = 0; i < postRecoveryCount; i++)
                    _ = bContext.Upsert(new TestObjectKey { key = 50_000 + i }, new TestObjectValue { value = 50_000 + i });

                store.Log.FlushAndEvict(wait: true);

                for (var i = 0; i < stableCount; i++)
                {
                    ClassicAssert.IsTrue(TryReadValue(bContext, i, out var value), $"stable key {i} not found");
                    ClassicAssert.AreEqual(i, value, $"stable key {i} has the wrong value");
                }
                for (var i = 0; i < postRecoveryCount; i++)
                {
                    ClassicAssert.IsTrue(TryReadValue(bContext, 50_000 + i, out var value), $"post-recovery key {50_000 + i} not found");
                    ClassicAssert.AreEqual(50_000 + i, value, $"post-recovery key {50_000 + i} has the wrong value");
                }
                for (var i = 0; i < nextVersionCount; i++)
                    ClassicAssert.IsFalse(TryReadValue(bContext, 10_000 + i, out _), $"(v+1) key {10_000 + i} was not undone");
            }
            finally
            {
                Destroy(log, objlog, store);
            }
        }

        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task FoldOverRecoveryThenPartialFlushKeepsBoundarySectorReadable()
        {
            const int stableCount = 500;
            const int postRecoveryCount = 100;
            var addresses = new long[stableCount + 256];
            int count;
            Guid token;

            Prepare(out var log, out var objlog, out var store);
            try
            {
                // Write until the tail is mid-sector, so the checkpoint's flush ends there and recovery's FlushedUntilAddress is mid-sector.
                var sectorSize = log.SectorSize;
                using var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, LenientFunctions>(new LenientFunctions());
                var bContext = session.BasicContext;
                for (count = 0; count < stableCount || store.Log.TailAddress % sectorSize == 0; count++)
                {
                    addresses[count] = store.Log.TailAddress;
                    _ = bContext.Upsert(new TestObjectKey { key = count }, new TestObjectValue { value = count });
                }
                ClassicAssert.IsTrue(store.TryInitiateHybridLogCheckpoint(out token, CheckpointType.FoldOver));
                await store.CompleteCheckpointAsync().AsTask().ConfigureAwait(false);
            }
            finally
            {
                Destroy(log, objlog, store);
            }

            Prepare(out log, out objlog, out store);
            try
            {
                _ = await store.RecoverAsync(default, token).ConfigureAwait(false);

                // The first flush after recovery starts in the recovered boundary sector; evicting everything makes every read come from disk.
                using var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, LenientFunctions>(new LenientFunctions());
                var bContext = session.BasicContext;
                for (var i = 0; i < postRecoveryCount; i++)
                    _ = bContext.Upsert(new TestObjectKey { key = 50_000 + i }, new TestObjectValue { value = 50_000 + i });
                store.Log.FlushAndEvict(wait: true);

                for (var i = 0; i < count; i++)
                {
                    ClassicAssert.IsTrue(TryReadValue(bContext, i, out var value), $"key {i} at {addresses[i]} not found");
                    ClassicAssert.AreEqual(i, value, $"key {i} at {addresses[i]} has the wrong value");
                }
            }
            finally
            {
                Destroy(log, objlog, store);
            }
        }

        static bool TryReadValue(BasicContext<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, LenientFunctions, ClassStoreFunctions, ClassAllocator> bContext,
            int key, out int value)
        {
            TestObjectInput input = default;
            TestObjectOutput output = new();
            var status = bContext.Read(new TestObjectKey { key = key }, ref input, ref output);
            if (status.IsPending)
            {
                Assert.That(bContext.CompletePendingWithOutputs(out var completedOutputs, wait: true), Is.True);
                (status, output) = GetSinglePendingResult(completedOutputs);
            }

            value = output.value?.value ?? -1;
            return status.Found;
        }

        static void Prepare(out IDevice log, out IDevice objlog, out TsavoriteKV<ClassStoreFunctions, ClassAllocator> store)
        {
            log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "tail.log"));
            objlog = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "tail.obj.log"));
            store = new(new()
            {
                IndexSize = 1L << 16,
                LogDevice = log,
                ObjectLogDevice = objlog,
                SegmentSize = 1L << 20,
                LogMemorySize = 64L * MinKvLogPageSize,
                PageSize = MinKvLogPageSize,
                CheckpointDir = Path.Combine(MethodTestDir, "check-points")
            }, StoreFunctions.Create(new TestObjectKey.Comparer(), () => new TestObjectValue.Serializer())
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));
        }

        static void Destroy(IDevice log, IDevice objlog, TsavoriteKV<ClassStoreFunctions, ClassAllocator> store)
        {
            store.Dispose();
            log.Dispose();
            objlog.Dispose();
        }
    }
}