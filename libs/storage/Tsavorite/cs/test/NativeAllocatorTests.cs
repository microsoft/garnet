// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;

namespace Tsavorite.test
{
    /// <summary>
    /// Tests for the direct-VM native allocator (native allocator enabled) that routes large regions
    /// (log pages / hash index / recovery frames) off the managed heap via <c>mmap</c>/<c>VirtualAlloc</c>.
    /// No native library is required — these call the OS virtual-memory APIs directly.
    /// </summary>
    [TestFixture]
    [NonParallelizable]
    public unsafe class NativeAllocatorTests
    {
        [Test]
        public void DirectVmAllocateIsZeroedAlignedWritable()
        {
            var block = DirectVirtualMemory.Allocate(1 << 20, 4096);   // 1 MB, 4 KB aligned
            try
            {
                ClassicAssert.IsFalse(block.IsEmpty);
                ClassicAssert.AreEqual(0, ((long)block.AlignedPtr) % 4096, "must be aligned");
                var span = new Span<byte>((void*)block.AlignedPtr, 1 << 20);
                foreach (var b in span)
                    ClassicAssert.AreEqual(0, b, "fresh OS mapping must be demand-zero");
                span.Fill(0xCD);
                ClassicAssert.AreEqual(0xCD, span[(1 << 20) - 1], "must be writable end-to-end");
            }
            finally
            {
                DirectVirtualMemory.Free(block);
            }
        }

        [Test]
        public void DirectVmTrackerReflectsAllocation()
        {
            var before = NativeMemoryTracker.Bytes;
            var block = DirectVirtualMemory.Allocate(8L << 20, 512);   // 8 MB
            var afterAlloc = NativeMemoryTracker.Bytes;
            ClassicAssert.GreaterOrEqual(afterAlloc - before, 8L << 20, "tracker should reflect the direct-VM reservation");
            DirectVirtualMemory.Free(block);
            var afterFree = NativeMemoryTracker.Bytes;
            ClassicAssert.Less(afterFree - before, 8L << 20, "tracker should drop after free");
        }
    }
}