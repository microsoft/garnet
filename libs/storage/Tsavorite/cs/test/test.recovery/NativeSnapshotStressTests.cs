// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.recovery
{
    using LongAllocator = SpanByteAllocator<StoreFunctions<LongKeyComparer, SpanByteRecordTriggers>>;
    using LongStoreFunctions = StoreFunctions<LongKeyComparer, SpanByteRecordTriggers>;

    /// <summary>
    /// Stress test targeting a code-review concern for the "full" native mode: a snapshot checkpoint writes
    /// directly from the direct-VM log-page pointers to the snapshot device, and (unlike a main-log flush) that
    /// IO does not gate main-log eviction. This hammers repeated snapshot checkpoints while background writers
    /// drive continuous flush+eviction through the same native pages, to surface any use-after-free (observed as
    /// an AccessViolationException) where an evicted page's block is unmapped while its snapshot write is still
    /// in flight. Explicit (slow) and process-global (flips the native surfaces), so NonParallelizable.
    /// </summary>
    [TestFixture]
    [NonParallelizable]
    internal class NativeSnapshotStressTests : TestBase
    {
        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            RecreateDirectory(MethodTestDir);
            _ = NativeAllocatorInitializer.Initialize(NativeAllocatorSurfaces.Full);
        }

        [TearDown]
        public void TearDown()
        {
            _ = NativeAllocatorInitializer.Initialize(NativeAllocatorSurfaces.None);
            DeleteDirectory(MethodTestDir);
        }

        [Test]
        [Explicit]
        [Category(TsavoriteKVTestCategory)]
        public void ConcurrentSnapshotAndEvictionStress()
        {
            using var log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "hlog.log"), deleteOnClose: true);
            using var store = new TsavoriteKV<LongStoreFunctions, LongAllocator>(new()
            {
                IndexSize = 1L << 20,
                LogDevice = log,
                PageSize = 1L << 12,        // 4 KB pages -> many native pages
                LogMemorySize = 1L << 17,   // 128 KB in-memory log -> constant flush + eviction
                MutableFraction = 0.1,      // tiny mutable region -> pages go read-only + flush quickly
                CheckpointDir = MethodTestDir
            }, StoreFunctions.Create(LongKeyComparer.Instance, SpanByteRecordTriggers.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));

            using var cts = new CancellationTokenSource();
            Exception writerError = null;

            // Background writers: continuous upserts drive the tail forward, forcing pages read-only -> flushed ->
            // evicted (ReturnPage -> FreeNativeLogPage) concurrently with the snapshot writes below.
            var writers = new Task[8];
            for (var w = 0; w < writers.Length; w++)
            {
                var seed = w;
                writers[w] = Task.Run(() =>
                {
                    try
                    {
                        using var s = store.NewSession<TestSpanByteKey, long, long, Empty, SimpleLongSimpleFunctions>(new SimpleLongSimpleFunctions());
                        var bc = s.BasicContext;
                        var keyArray = new byte[sizeof(long)];
                        long key = seed;
                        while (!cts.IsCancellationRequested)
                        {
                            var keySpan = new Span<byte>(keyArray);
                            keySpan.AsRef<long>() = key;
                            _ = bc.Upsert(TestSpanByteKey.FromArray(keyArray), keySpan);
                            key += writers.Length;
                            if (key > 200_000)
                                key = seed;
                        }
                    }
                    catch (Exception ex)
                    {
                        writerError = ex;
                    }
                });
            }

            // Main: repeated snapshot checkpoints overlapping the eviction churn above.
            const int checkpoints = 200;
            for (var i = 0; i < checkpoints; i++)
            {
                var (success, _) = store.TakeHybridLogCheckpointAsync(CheckpointType.Snapshot).AsTask().GetAwaiter().GetResult();
                ClassicAssert.IsTrue(success, $"snapshot checkpoint {i} failed");
                store.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();
            }

            cts.Cancel();
            Task.WaitAll(writers);
            ClassicAssert.IsNull(writerError, $"writer failed: {writerError}");
        }
    }
}