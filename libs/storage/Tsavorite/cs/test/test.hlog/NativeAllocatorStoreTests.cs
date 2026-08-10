// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Runtime.InteropServices;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.spanbyte
{
    using SpanByteStoreFunctions = StoreFunctions<SpanByteComparer, SpanByteRecordTriggers>;

    /// <summary>
    /// Parametrizes a real store's insert / read / update / delete + checkpoint-recover round-trip across all three
    /// native-allocator modes (off | buffer-pool | full), so the same behavior is validated on the managed backend
    /// and both native backends (mimalloc buffer pool + direct-VM index/log/frames). Complements the surface-specific
    /// fixtures (NativeAllocatorTests, NativeHashIndexTests, NativeSnapshotStressTests) with an end-to-end store path.
    ///
    /// The surfaces are a process-global installed once per test via <see cref="NativeAllocatorInitializer"/>, so the
    /// fixture is <see cref="NonParallelizableAttribute"/> and resets to managed in teardown. Native modes require the
    /// shipped mimalloc for the current RID; the run is skipped (not failed) when it cannot load, keeping CI green on
    /// RIDs that do not yet ship the binary.
    /// </summary>
    [TestFixture]
    [NonParallelizable]
    internal class NativeAllocatorStoreTests : TestBase
    {
        // Named cases so the test IDs read as (Off) / (BufferPool) / (Full).
        static readonly object[] AllocatorModes =
        [
            NativeAllocatorSurfaces.None,
            NativeAllocatorSurfaces.BufferPool,
            NativeAllocatorSurfaces.Full,
        ];

        [SetUp]
        public void Setup() => DeleteDirectory(MethodTestDir, wait: true);

        [TearDown]
        public void TearDown()
        {
            _ = NativeAllocatorInitializer.Initialize(NativeAllocatorSurfaces.None);
            // These tests create Full-mode stores whose direct-VM index/log blocks are freed lazily by the
            // NativePageBlockRegistry finalizer. Drain finalizers here so those deferred NativeMemoryTracker
            // Subtract calls do not fire during a later test (which would corrupt its tracker delta or leave a
            // transient LightEpoch), keeping the process-global native state clean between tests.
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            DeleteDirectory(MethodTestDir);
        }

        static void EnableOrSkip(NativeAllocatorSurfaces mode)
        {
            // Any non-off mode here includes/needs the mimalloc-backed BufferPool (Full = BufferPool | store surfaces),
            // so require mimalloc; skip on an unshipped RID rather than fail fast.
            if (mode != NativeAllocatorSurfaces.None && !Mimalloc.TryInitialize())
                Assert.Ignore("mimalloc native library not available for this RID");
            _ = NativeAllocatorInitializer.Initialize(mode);
        }

        static TsavoriteKV<SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>> CreateStore(IDevice log)
            => new(new()
            {
                IndexSize = 1L << 20,
                LogDevice = log,
                LogMemorySize = 1L << 20,   // small in-memory log -> flush + eviction through the (native) buffers/pages
                PageSize = 1L << 14,
                CheckpointDir = MethodTestDir
            }, StoreFunctions.Create(SpanByteComparer.Instance, SpanByteRecordTriggers.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));

        [Test]
        [Category(TsavoriteKVTestCategory)]
        [TestCaseSource(nameof(AllocatorModes))]
        public void InsertReadUpdateDelete(NativeAllocatorSurfaces mode)
        {
            EnableOrSkip(mode);
            using var log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "hlog.log"), deleteOnClose: true);
            using var store = CreateStore(log);
            using var session = store.NewSession<TestSpanByteKey, PinnedSpanByte, int[], Empty, VLVectorFunctions>(new VLVectorFunctions());
            var bContext = session.BasicContext;

            void Upsert(int key, int val)
            {
                Span<int> k = [key];
                Span<int> v = stackalloc int[4]; for (var j = 0; j < 4; j++) v[j] = val;
                _ = bContext.Upsert(TestSpanByteKey.FromPinnedSpan(MemoryMarshal.Cast<int, byte>(k)), MemoryMarshal.Cast<int, byte>(v), Empty.Default);
            }
            bool TryRead(int key, out int first)
            {
                Span<int> k = [key];
                int[] output = null;
                var status = bContext.Read(TestSpanByteKey.FromPinnedSpan(MemoryMarshal.Cast<int, byte>(k)), ref output, Empty.Default);
                if (status.IsPending)
                {
                    _ = bContext.CompletePendingWithOutputs(out var outputs, wait: true);
                    (status, output) = GetSinglePendingResult(outputs);
                }
                first = status.Found ? output[0] : default;
                return status.Found;
            }

            const int n = 20_000;
            for (var i = 0; i < n; i++)
                Upsert(i, i);
            for (var i = 0; i < n; i++)
            {
                ClassicAssert.IsTrue(TryRead(i, out var v), $"[{mode}] key {i} not found after insert");
                ClassicAssert.AreEqual(i, v);
            }

            // Update in place / RCU, then re-read.
            for (var i = 0; i < n; i++)
                Upsert(i, i + 1);
            for (var i = 0; i < n; i++)
            {
                ClassicAssert.IsTrue(TryRead(i, out var v), $"[{mode}] key {i} not found after update");
                ClassicAssert.AreEqual(i + 1, v);
            }

            // Delete a slice and confirm it is gone.
            Span<int> dk = stackalloc int[1];
            for (var i = 0; i < n; i += 2)
            {
                dk[0] = i;
                _ = bContext.Delete(TestSpanByteKey.FromPinnedSpan(MemoryMarshal.Cast<int, byte>(dk)));
            }
            for (var i = 0; i < n; i++)
            {
                var found = TryRead(i, out var v);
                if ((i & 1) == 0)
                    ClassicAssert.IsFalse(found, $"[{mode}] deleted key {i} still present");
                else
                {
                    ClassicAssert.IsTrue(found, $"[{mode}] key {i} missing after neighbor delete");
                    ClassicAssert.AreEqual(i + 1, v);
                }
            }
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        [TestCaseSource(nameof(AllocatorModes))]
        public void CheckpointRecover(NativeAllocatorSurfaces mode)
        {
            EnableOrSkip(mode);
            const int n = 10_000;
            Guid token;

            void UpsertAll(TsavoriteKV<SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>> store)
            {
                using var session = store.NewSession<TestSpanByteKey, PinnedSpanByte, int[], Empty, VLVectorFunctions>(new VLVectorFunctions());
                var bContext = session.BasicContext;
                Span<int> k = stackalloc int[1];
                Span<int> v = stackalloc int[4];
                for (var i = 0; i < n; i++)
                {
                    k[0] = i; for (var j = 0; j < 4; j++) v[j] = i;
                    _ = bContext.Upsert(TestSpanByteKey.FromPinnedSpan(MemoryMarshal.Cast<int, byte>(k)), MemoryMarshal.Cast<int, byte>(v), Empty.Default);
                }
            }

            void VerifyAll(TsavoriteKV<SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>> store)
            {
                using var session = store.NewSession<TestSpanByteKey, PinnedSpanByte, int[], Empty, VLVectorFunctions>(new VLVectorFunctions());
                var bContext = session.BasicContext;
                Span<int> k = stackalloc int[1];
                for (var i = 0; i < n; i++)
                {
                    k[0] = i;
                    int[] output = null;
                    var status = bContext.Read(TestSpanByteKey.FromPinnedSpan(MemoryMarshal.Cast<int, byte>(k)), ref output, Empty.Default);
                    if (status.IsPending)
                    {
                        _ = bContext.CompletePendingWithOutputs(out var outputs, wait: true);
                        (status, output) = GetSinglePendingResult(outputs);
                    }
                    ClassicAssert.IsTrue(status.Found, $"[{mode}] key {i} not recovered");
                    ClassicAssert.AreEqual(i, output[0]);
                }
            }

            using (var log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "hlog.log"), deleteOnClose: false))
            using (var store = CreateStore(log))
            {
                UpsertAll(store);
                // Snapshot checkpoint: flushes route through the (native, in BufferPool/Full) sector-aligned buffers;
                // in Full mode this also reads the direct-VM log pages.
                var initiated = store.TryInitiateFullCheckpoint(out token, CheckpointType.Snapshot);
                ClassicAssert.IsTrue(initiated, $"[{mode}] checkpoint not initiated");
                store.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();
            }

            // Fresh store recovers from the checkpoint (Full: rebuilds direct-VM index + reads via direct-VM frames).
            using (var log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "hlog.log"), deleteOnClose: true))
            using (var store = CreateStore(log))
            {
                _ = store.RecoverAsync(token).AsTask().GetAwaiter().GetResult();
                VerifyAll(store);
            }
        }
    }
}