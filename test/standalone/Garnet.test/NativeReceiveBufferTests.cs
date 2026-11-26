// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using System.Threading;
using Garnet.common;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;

namespace Garnet.test
{
    /// <summary>
    /// Tests the lifetime of the native memory behind receive buffers above the network buffer pool's largest size class.
    /// </summary>
    [TestFixture]
    [NonParallelizable]
    public class NativeReceiveBufferTests : TestBase
    {
        const int Size = 32 << 20;

        /// <summary>A pool whose largest size class is 4 KB, so that the buffers here are native.</summary>
        static LimitedFixedBufferPool CreatePool() => new(minAllocationSize: 4096, numLevels: 1);

        static NativeReceiveBuffer CreateInstalled(LimitedFixedBufferPool pool, int size)
        {
            var buffer = new NativeReceiveBuffer(pool);
            buffer.Install(buffer.Map(size), size);
            return buffer;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        static void InstallAndReleaseDeferred(LimitedFixedBufferPool pool) => CreateInstalled(pool, Size).ReleaseDeferred();

        [MethodImpl(MethodImplOptions.NoInlining)]
        static MemoryHandle InstallReleaseDeferredAndPin(LimitedFixedBufferPool pool)
        {
            var buffer = CreateInstalled(pool, Size);
            buffer.ReleaseDeferred();
            return buffer.Memory.Pin();
        }

        /// <summary>
        /// Pins a buffer left to the finalizer, checks a collection leaves it mapped, then releases the handle. A method of its own, so that no
        /// copy of the handle outlives it.
        /// </summary>
        [MethodImpl(MethodImplOptions.NoInlining)]
        static void PinCollectAndRelease(LimitedFixedBufferPool pool, long before)
        {
            var handle = InstallReleaseDeferredAndPin(pool);
            GC.Collect();
            GC.WaitForPendingFinalizers();
            ClassicAssert.GreaterOrEqual(NativeMemoryTracker.Bytes - before, Size, "the buffer was unmapped while a handle pinned it");
            handle.Dispose();
        }

        /// <summary>Grows a buffer from native to larger native sizes, as a large message arrives, then moves it back into the pool's sizes.</summary>
        static void GrowAndShrink(NativeReceiveBuffer buffer)
        {
            for (var size = 2 << 20; size <= 128 << 20; size <<= 3)
                buffer.Install(buffer.Map(size), size);
            buffer.Unmap();
        }

        [Test]
        public void GrowingAndShrinkingAllocatesNothingAndReleasesEachRegion()
        {
            using var pool = CreatePool();
            var buffer = new NativeReceiveBuffer(pool);
            var before = NativeMemoryTracker.Bytes;

            // The first pass binds the native calls
            GrowAndShrink(buffer);
            const int Cycles = 50, Regions = 3 * Cycles;
            var allocated = GC.GetAllocatedBytesForCurrentThread();
            for (var i = 0; i < Cycles; i++)
                GrowAndShrink(buffer);
            allocated = GC.GetAllocatedBytesForCurrentThread() - allocated;

            // The runtime can allocate a few bytes on this thread once, such as when it recompiles hot methods, but an object per region is at
            // least 24 bytes.
            ClassicAssert.Less(allocated, 8 * Regions, $"growing and shrinking a native receive buffer allocated {allocated} bytes over {Regions} regions");
            ClassicAssert.AreEqual(0, pool.LiveBytes, "a replaced or unmapped region is still accounted to the pool");
            ClassicAssert.AreEqual(before, NativeMemoryTracker.Bytes, "a replaced or unmapped region is still mapped");
        }

        /// <summary>
        /// A handler disposed while its receive thread replaces the buffer releases the region installed at the time; the receive thread then
        /// installs another, which its own release after the replacement must account for too.
        /// </summary>
        [Test]
        public void ARegionInstalledAfterADeferredReleaseIsReleasedByTheNext()
        {
            var pool = CreatePool();
            var before = NativeMemoryTracker.Bytes;
            var buffer = CreateInstalled(pool, Size);
            try
            {
                buffer.ReleaseDeferred();
                ClassicAssert.AreEqual(0, pool.LiveBytes);

                // Each mapping reserves a little more than its size, for alignment
                buffer.Install(buffer.Map(2 * Size), 2 * Size);
                var mapped = NativeMemoryTracker.Bytes - before;
                ClassicAssert.GreaterOrEqual(mapped, 2 * Size);
                ClassicAssert.Less(mapped, 3 * Size, "the region released by the disposal was not unmapped when replaced");

                buffer.ReleaseDeferred();
                buffer.ReleaseDeferred();
                ClassicAssert.AreEqual(0, pool.LiveBytes, "the region installed after the disposal is still accounted to the pool");
            }
            finally
            {
                // Releases any accounting left by a failure, which would otherwise keep the pool from disposing
                buffer.Unmap();
                pool.Dispose();
            }
        }

        /// <summary>
        /// A connection disposed while it holds a native buffer leaves the memory to the finalizer. Allocating nothing else, buffers left that
        /// way must still prompt the collection that frees them.
        /// </summary>
        [Test]
        public void BuffersLeftToTheFinalizerPromptACollection()
        {
            using var pool = CreatePool();
            var before = NativeMemoryTracker.Bytes;

            const int Count = 64;
            for (var i = 0; i < Count; i++)
            {
                InstallAndReleaseDeferred(pool);
                Thread.Sleep(10);
            }

            // No collection is forced: only the memory reported by the buffers can have prompted one.
            GC.WaitForPendingFinalizers();
            var retained = NativeMemoryTracker.Bytes - before;
            ClassicAssert.Less(retained, (long)Count * Size / 2, $"{retained} bytes of buffers left to the finalizer are still mapped");
        }

        [Test]
        public void APinnedBufferStaysMappedUntilTheHandleIsReleased()
        {
            using var pool = CreatePool();
            GC.Collect();
            GC.WaitForPendingFinalizers();
            var before = NativeMemoryTracker.Bytes;

            PinCollectAndRelease(pool, before);
            GC.Collect();
            GC.WaitForPendingFinalizers();
            ClassicAssert.Less(NativeMemoryTracker.Bytes - before, Size, "the buffer was not freed once its handle was released");
        }
    }
}