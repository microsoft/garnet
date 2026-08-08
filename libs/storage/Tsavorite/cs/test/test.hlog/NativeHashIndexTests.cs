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
    /// End-to-end tests for the direct-VM (mmap/VirtualAlloc) hash index — the native-allocator "full" mode
    /// HashIndex surface. Flips the process-global <see cref="NativeAllocatorInitializer.EnabledSurfaces"/>, so
    /// this fixture is <see cref="NonParallelizableAttribute"/> and resets it in teardown.
    /// </summary>
    [TestFixture]
    [NonParallelizable]
    internal class NativeHashIndexTests : TestBase
    {
        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            _ = NativeAllocatorInitializer.Initialize(NativeAllocatorSurfaces.HashIndex);
        }

        [TearDown]
        public void TearDown()
        {
            _ = NativeAllocatorInitializer.Initialize(NativeAllocatorSurfaces.None);
            DeleteDirectory(MethodTestDir);
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        public void NativeHashIndexInsertReadRoundTrips()
        {
            var before = NativeMemoryTracker.Bytes;
            var log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "hlog.log"), deleteOnClose: true);
            var store = new TsavoriteKV<SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>>(
                new()
                {
                    IndexSize = 1L << 22,   // 4 MB index -> a meaningful direct-VM reservation (> one page)
                    LogDevice = log,
                    LogMemorySize = 1L << 20,
                    PageSize = 1L << 14
                }, StoreFunctions.Create(SpanByteComparer.Instance, SpanByteRecordTriggers.Instance)
                    , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));

            // The index table is now a direct-VM reservation, so tracked native bytes must have grown.
            ClassicAssert.GreaterOrEqual(NativeMemoryTracker.Bytes - before, 1L << 22,
                "hash index should be backed by direct virtual memory");

            var session = store.NewSession<TestSpanByteKey, PinnedSpanByte, int[], Empty, VLVectorFunctions>(new VLVectorFunctions());
            var bContext = session.BasicContext;

            Span<int> keySpan = stackalloc int[1];
            Span<int> valueSpan = stackalloc int[4];

            const int n = 20_000;
            for (var i = 0; i < n; i++)
            {
                keySpan[0] = i;
                for (var j = 0; j < 4; j++)
                    valueSpan[j] = i;
                _ = bContext.Upsert(TestSpanByteKey.FromPinnedSpan(MemoryMarshal.Cast<int, byte>(keySpan)),
                    MemoryMarshal.Cast<int, byte>(valueSpan), Empty.Default);
            }

            for (var i = 0; i < n; i++)
            {
                keySpan[0] = i;
                int[] output = null;
                var status = bContext.Read(TestSpanByteKey.FromPinnedSpan(MemoryMarshal.Cast<int, byte>(keySpan)), ref output, Empty.Default);
                if (status.IsPending)
                {
                    _ = bContext.CompletePendingWithOutputs(out var outputs, wait: true);
                    (status, output) = GetSinglePendingResult(outputs);
                }
                ClassicAssert.IsTrue(status.Found, $"key {i} not found");
                ClassicAssert.AreEqual(4, output.Length);
                ClassicAssert.AreEqual(i, output[0]);
            }

            session.Dispose();
            store.Dispose();
            log.Dispose();

            // After Dispose, the direct-VM index must be released (tracked native bytes back to baseline).
            ClassicAssert.Less(NativeMemoryTracker.Bytes - before, 1L << 22, "index native memory should be freed on Dispose");
        }
    }
}
