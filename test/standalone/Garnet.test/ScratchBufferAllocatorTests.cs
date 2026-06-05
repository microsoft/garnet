// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    [TestFixture]
    public class ScratchBufferAllocatorTests : TestBase
    {
        // A no-grow savepoint rewind reclaims the scratch space and leaves earlier slices intact.
        [Test]
        public void NoGrowRewindReclaimsOffsetAndPreservesEarlierData()
        {
            var sba = new ScratchBufferAllocator();

            // Pre-grow to a buffer large enough that the subsequent small allocations don't grow it.
            _ = sba.CreateArgSlice(4096);
            sba.Reset();
            var bufferLength = sba.TotalLength;

            // Slice from a notional earlier command in the same batch; it must survive the rewind.
            var prior = sba.CreateArgSlice(16);
            prior.Span.Fill(0xAB);

            var savedOffset = sba.ScratchBufferOffset;
            var savedCount = sba.RetainedBufferCount;

            // Reserve several "GET_SG slots" on top.
            for (var i = 0; i < 8; i++)
                sba.CreateArgSlice(16).Span.Fill((byte)(i + 1));

            ClassicAssert.Greater(sba.ScratchBufferOffset, savedOffset);
            ClassicAssert.AreEqual(savedCount, sba.RetainedBufferCount, "should not have grown");

            sba.TryRewindToOffset(savedOffset, savedCount);

            ClassicAssert.AreEqual(savedOffset, sba.ScratchBufferOffset, "offset reclaimed");
            foreach (var b in prior.ReadOnlySpan)
                ClassicAssert.AreEqual(0xAB, b, "earlier slice preserved");

            // Next allocation reuses the reclaimed space rather than growing.
            _ = sba.CreateArgSlice(16);
            ClassicAssert.AreEqual(savedOffset + 16, sba.ScratchBufferOffset);
            ClassicAssert.AreEqual(bufferLength, sba.TotalLength, "no growth on reuse");
        }

        // When the allocator grows within the scope, the rewind is skipped (the larger buffer is retained).
        [Test]
        public void GrowDuringScopeSkipsRewind()
        {
            var sba = new ScratchBufferAllocator();

            _ = sba.CreateArgSlice(16);
            var savedOffset = sba.ScratchBufferOffset;
            var savedCount = sba.RetainedBufferCount;
            var bufferLength = sba.TotalLength;

            // Allocate more than the remaining capacity to force a grow (pushes a buffer).
            _ = sba.CreateArgSlice(bufferLength);
            ClassicAssert.Greater(sba.RetainedBufferCount, savedCount, "should have grown");

            var offsetBeforeRewind = sba.ScratchBufferOffset;
            sba.TryRewindToOffset(savedOffset, savedCount);
            ClassicAssert.AreEqual(offsetBeforeRewind, sba.ScratchBufferOffset, "rewind skipped on grow");
        }

        // Rewinding back to the very first (default) state reclaims to zero but keeps the grown buffer.
        [Test]
        public void FirstAllocationFromDefaultRewindsToZeroAndRetainsBuffer()
        {
            var sba = new ScratchBufferAllocator();

            var savedOffset = sba.ScratchBufferOffset; // 0
            var savedCount = sba.RetainedBufferCount;  // 0

            _ = sba.CreateArgSlice(64);
            ClassicAssert.AreEqual(savedCount, sba.RetainedBufferCount, "first alloc from default does not push");
            ClassicAssert.Greater(sba.ScratchBufferOffset, 0);

            sba.TryRewindToOffset(savedOffset, savedCount);
            ClassicAssert.AreEqual(0, sba.ScratchBufferOffset);
            ClassicAssert.Greater(sba.TotalLength, 0, "buffer retained for reuse");
        }

        // Reserve-then-rewind across many runs must not grow the retained memory (mixed-pipeline case).
        [Test]
        public void RepeatedReserveRewindDoesNotGrow()
        {
            var sba = new ScratchBufferAllocator();

            // Warm up to a buffer that fits one run of 8x128, then reset.
            for (var i = 0; i < 8; i++)
                _ = sba.CreateArgSlice(128);
            sba.Reset();
            var boundedTotal = sba.TotalLength;

            for (var run = 0; run < 1000; run++)
            {
                var savedOffset = sba.ScratchBufferOffset;
                var savedCount = sba.RetainedBufferCount;
                for (var i = 0; i < 8; i++)
                    _ = sba.CreateArgSlice(128);
                sba.TryRewindToOffset(savedOffset, savedCount);
            }

            ClassicAssert.AreEqual(0, sba.ScratchBufferOffset);
            ClassicAssert.AreEqual(boundedTotal, sba.TotalLength, "retained memory stayed bounded across runs");
        }
    }
}