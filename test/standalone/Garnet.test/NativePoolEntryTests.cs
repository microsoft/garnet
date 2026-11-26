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
    public class NativePoolEntryTests : TestBase
    {
        const int Size = 32 << 20;

        /// <summary>A pool whose largest size class is 4 KB, so that the buffers here are native.</summary>
        static LimitedFixedBufferPool CreatePool() => new(minAllocationSize: 4096, numLevels: 1);

        [MethodImpl(MethodImplOptions.NoInlining)]
        static void GetAndReleaseDeferred(LimitedFixedBufferPool pool) => pool.GetNative(Size).ReleaseDeferred();

        [MethodImpl(MethodImplOptions.NoInlining)]
        static MemoryHandle GetReleaseDeferredAndPin(LimitedFixedBufferPool pool)
        {
            var entry = pool.GetNative(Size);
            entry.ReleaseDeferred();
            return entry.Memory.Pin();
        }

        /// <summary>
        /// Pins a buffer left to the finalizer, checks a collection leaves it mapped, then releases the handle. A method of its own, so that no
        /// copy of the handle outlives it.
        /// </summary>
        [MethodImpl(MethodImplOptions.NoInlining)]
        static void PinCollectAndRelease(LimitedFixedBufferPool pool, long before)
        {
            var handle = GetReleaseDeferredAndPin(pool);
            GC.Collect();
            GC.WaitForPendingFinalizers();
            ClassicAssert.GreaterOrEqual(NativeMemoryTracker.Bytes - before, Size, "the buffer was unmapped while a handle pinned it");
            handle.Dispose();
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
                GetAndReleaseDeferred(pool);
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