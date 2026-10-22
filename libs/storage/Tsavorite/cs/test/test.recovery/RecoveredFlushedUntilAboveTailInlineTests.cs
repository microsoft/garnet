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

namespace Tsavorite.test.recovery
{
    using LongAllocator = SpanByteAllocator<StoreFunctions<LongKeyComparer, SpanByteRecordTriggers>>;
    using LongStoreFunctions = StoreFunctions<LongKeyComparer, SpanByteRecordTriggers>;

    /// <summary>
    /// A Snapshot checkpoint whose ReadOnly flush passes the checkpoint's tail before PERSISTENCE_CALLBACK must not recover
    /// with FlushedUntilAddress (and ReadOnlyAddress) above TailAddress; records written after recovery would otherwise be
    /// treated as durable, never flushed, and lost on eviction. Inline values keep this independent of object-log handling.
    /// </summary>
    [TestFixture]
    public class RecoveredFlushedUntilAboveTailInlineTests : TestBase
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

        [SetUp]
        public void Setup() => RecreateDirectory(MethodTestDir);

        [TearDown]
        public void TearDown() => OnTearDown();

        /// <param name="unclampedMetadata">Rewrite the checkpoint metadata with the unclamped value a checkpoint written before the
        /// clamp would carry, so recovery alone must clamp it.</param>
        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task ReadOnlyFlushPastCheckpointTailDoesNotRecoverAboveTail([Values] bool unclampedMetadata)
        {
            const int stableCount = 500;
            const int nextVersionCount = 2000;
            const int postRecoveryCount = 100;
            Guid token;

            var log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "tail.log"));
            var store = CreateStore(log);
            try
            {
                using var session = store.NewSession<TestSpanByteKey, long, long, Empty, SimpleLongSimpleFunctions>(new SimpleLongSimpleFunctions());
                var bContext = session.BasicContext;
                for (var i = 0; i < stableCount; i++)
                    Upsert(bContext, i, i);

                // Callbacks run after the task hooks, so this holds the machine after the WAIT_FLUSH hook (tail sampled, Snapshot
                // issued) and before the PERSISTENCE_CALLBACK hook samples FlushedUntilAddress. IN_PROGRESS stays published.
                var pause = new PauseBeforePhase(Phase.WAIT_FLUSH);
                store.stateMachineDriver.UnsafeRegisterCallback(pause);
                ClassicAssert.IsTrue(store.TryInitiateHybridLogCheckpoint(out token, CheckpointType.Snapshot));
                Assert.That(pause.Reached.Wait(TimeSpan.FromSeconds(30)), Is.True, "never reached WAIT_FLUSH");

                // (v+1) writes move the tail well past the checkpoint's tail; flushing them moves FlushedUntilAddress past it too.
                long checkpointTail, flushedPastTail;
                try
                {
                    checkpointTail = store._hybridLogCheckpoint.info.recoveredTailAddress;
                    for (var i = 0; i < nextVersionCount; i++)
                        Upsert(bContext, 10_000 + i, 10_000 + i);
                    store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
                    flushedPastTail = store.Log.FlushedUntilAddress;
                    Assert.That(flushedPastTail, Is.GreaterThan(checkpointTail), "ReadOnly flush did not pass the checkpoint tail");
                }
                finally
                {
                    // Release the state machine even on failure, so disposing the store does not wait out the pause.
                    pause.Release.Set();
                }
                await store.CompleteCheckpointAsync().AsTask().ConfigureAwait(false);

                HybridLogCheckpointInfo persisted = default;
                try
                {
                    persisted.Recover(token, store.checkpointManager);
                    Assert.That(persisted.info.mainLogRecoveryEndAddress, Is.LessThanOrEqualTo(checkpointTail), "checkpoint persisted a recovery end above its tail");
                    if (unclampedMetadata)
                    {
                        persisted.info.mainLogRecoveryEndAddress = flushedPastTail;
                        store.checkpointManager.CommitLogCheckpointMetadata(token, persisted.info.ToByteArray());
                    }
                }
                finally
                {
                    persisted.Dispose();
                }
            }
            finally
            {
                store.Dispose();
                log.Dispose();
            }

            log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "tail.log"));
            store = CreateStore(log);
            try
            {
                if (unclampedMetadata)
                {
                    HybridLogCheckpointInfo rewritten = default;
                    rewritten.Recover(token, store.checkpointManager);
                    Assert.That(rewritten.info.mainLogRecoveryEndAddress, Is.GreaterThan(rewritten.info.recoveredTailAddress), "metadata rewrite did not take effect");
                    rewritten.Dispose();
                }

                _ = await store.RecoverAsync(default, token).ConfigureAwait(false);
                Assert.That(store.Log.ReadOnlyAddress, Is.LessThanOrEqualTo(store.Log.TailAddress),
                    $"Recovered ReadOnlyAddress {store.Log.ReadOnlyAddress} (FlushedUntilAddress {store.Log.FlushedUntilAddress}) is above TailAddress {store.Log.TailAddress}");

                using var session = store.NewSession<TestSpanByteKey, long, long, Empty, SimpleLongSimpleFunctions>(new SimpleLongSimpleFunctions());
                var bContext = session.BasicContext;
                for (var i = 0; i < postRecoveryCount; i++)
                    Upsert(bContext, 50_000 + i, 50_000 + i);

                store.Log.FlushAndEvict(wait: true);

                for (var i = 0; i < stableCount; i++)
                {
                    ClassicAssert.IsTrue(TryRead(bContext, i, out var value), $"stable key {i} not found");
                    ClassicAssert.AreEqual(i, value, $"stable key {i} has the wrong value");
                }
                for (var i = 0; i < postRecoveryCount; i++)
                {
                    ClassicAssert.IsTrue(TryRead(bContext, 50_000 + i, out var value), $"post-recovery key {50_000 + i} not found");
                    ClassicAssert.AreEqual(50_000 + i, value, $"post-recovery key {50_000 + i} has the wrong value");
                }
                for (var i = 0; i < nextVersionCount; i++)
                    ClassicAssert.IsFalse(TryRead(bContext, 10_000 + i, out _), $"(v+1) key {10_000 + i} was not undone");
            }
            finally
            {
                store.Dispose();
                log.Dispose();
            }
        }

        static TsavoriteKV<LongStoreFunctions, LongAllocator> CreateStore(IDevice log) => new(new()
        {
            IndexSize = 1L << 16,
            LogDevice = log,
            PageSize = 1L << 12,
            LogMemorySize = 1L << 20,
            CheckpointDir = Path.Combine(MethodTestDir, "check-points")
        }, StoreFunctions.Create(LongKeyComparer.Instance, SpanByteRecordTriggers.Instance)
            , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));

        static void Upsert(BasicContext<TestSpanByteKey, long, long, Empty, SimpleLongSimpleFunctions, LongStoreFunctions, LongAllocator> bContext, long key, long value)
        {
            var keyArray = new byte[sizeof(long)];
            var valueArray = new byte[sizeof(long)];
            new Span<byte>(keyArray).AsRef<long>() = key;
            new Span<byte>(valueArray).AsRef<long>() = value;
            _ = bContext.Upsert(TestSpanByteKey.FromArray(keyArray), new Span<byte>(valueArray));
        }

        static bool TryRead(BasicContext<TestSpanByteKey, long, long, Empty, SimpleLongSimpleFunctions, LongStoreFunctions, LongAllocator> bContext, long key, out long value)
        {
            var keyArray = new byte[sizeof(long)];
            new Span<byte>(keyArray).AsRef<long>() = key;
            long output = default;
            var status = bContext.Read(TestSpanByteKey.FromArray(keyArray), ref output);
            if (status.IsPending)
            {
                Assert.That(bContext.CompletePendingWithOutputs(out var completedOutputs, wait: true), Is.True);
                (status, output) = GetSinglePendingResult(completedOutputs);
            }
            value = output;
            return status.Found;
        }
    }
}