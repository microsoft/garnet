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
        [Test]

        public void CreateArgSliceAndRewindTest([Values(0, 127, 8192)] int maxInitialCapacity)
        {
            var string1 = new string('a', 5);
            var string2 = new string('b', 65);
            var string3 = new string('c', 6000);

            var sam = new ScratchBufferAllocator(minSizeBuffer: 2, maxInitialCapacity: maxInitialCapacity);

            // Data of length 5 - SAM creates a buffer of size 8
            var as1 = sam.CreateArgSlice(string1);
            ClassicAssert.AreEqual(string1.Length, sam.ScratchBufferOffset);
            ClassicAssert.AreEqual(8, sam.TotalLength);

            // Data of length 65 - SAM creates an additional buffer of size 128
            var as2 = sam.CreateArgSlice(string2);
            ClassicAssert.AreEqual(string1.Length + string2.Length, sam.ScratchBufferOffset);
            ClassicAssert.AreEqual(8 + 128, sam.TotalLength);

            // Cannot re-wind slice that was not the last one created
            ClassicAssert.IsFalse(sam.RewindScratchBuffer(ref as1));

            // Re-wind last slice created - new offset is 5
            // Total length is 8 + 128 (last buffer is empty but still around)
            ClassicAssert.IsTrue(sam.RewindScratchBuffer(ref as2));
            ClassicAssert.AreEqual(string1.Length, sam.ScratchBufferOffset);
            ClassicAssert.AreEqual(8 + 128, sam.TotalLength);

            // Re-wind last slice created - new offset is 0
            // Total length either 0 or 8 (depending on if the initial capacity max is >= 8)
            ClassicAssert.IsTrue(sam.RewindScratchBuffer(ref as1));
            ClassicAssert.AreEqual(0, sam.ScratchBufferOffset);
            var expectedTotalSize = maxInitialCapacity switch
            {
                < 8 => 0,
                _ => 8
            };
            ClassicAssert.AreEqual(expectedTotalSize, sam.TotalLength);

            // Re-create slices for previous data
            // Total length is 8 + 128
            _ = sam.CreateArgSlice(string1);
            _ = sam.CreateArgSlice(string2);
            ClassicAssert.AreEqual(8 + 128, sam.TotalLength);

            // Reset all buffers, offset should be 0
            // Total length is either 0, 8 or 128 (depending on the initial capacity max)
            sam.Reset();
            ClassicAssert.AreEqual(0, sam.ScratchBufferOffset);
            expectedTotalSize = maxInitialCapacity switch
            {
                0 => 0,
                < 128 => 8,
                _ => 128
            };
            ClassicAssert.AreEqual(expectedTotalSize, sam.TotalLength);

            // Data of length 6611 - SAM creates a buffer of size 8192 & removes the current empty buffer
            var as3 = sam.CreateArgSlice(string3);
            // Data of length 5 - added to the existing buffer
            // Total length either 8192
            as1 = sam.CreateArgSlice(string1);
            ClassicAssert.AreEqual(string1.Length + string3.Length, sam.ScratchBufferOffset);
            ClassicAssert.AreEqual(8192, sam.TotalLength);

            // Re-wind last 2 slices created - new offset is 0
            // Total length is either 0 or 8192 (depending on the initial capacity max)
            ClassicAssert.IsTrue(sam.RewindScratchBuffer(ref as1));
            ClassicAssert.IsTrue(sam.RewindScratchBuffer(ref as3));
            ClassicAssert.AreEqual(0, sam.ScratchBufferOffset);
            expectedTotalSize = maxInitialCapacity switch
            {
                < 8192 => 0,
                _ => 8192
            };
            ClassicAssert.AreEqual(expectedTotalSize, sam.TotalLength);
        }

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

            // Reserve several "GET_SG slots" on top.
            for (var i = 0; i < 8; i++)
                sba.CreateArgSlice(16).Span.Fill((byte)(i + 1));

            ClassicAssert.Greater(sba.ScratchBufferOffset, savedOffset);
            ClassicAssert.AreEqual(bufferLength, sba.TotalLength, "should not have grown");

            sba.TryRewindToOffset(savedOffset);

            ClassicAssert.AreEqual(savedOffset, sba.ScratchBufferOffset, "offset reclaimed");
            foreach (var b in prior.ReadOnlySpan)
                ClassicAssert.AreEqual(0xAB, b, "earlier slice preserved");

            // Next allocation reuses the reclaimed space rather than growing.
            _ = sba.CreateArgSlice(16);
            ClassicAssert.AreEqual(savedOffset + 16, sba.ScratchBufferOffset);
            ClassicAssert.AreEqual(bufferLength, sba.TotalLength, "no growth on reuse");
        }

        // When the allocator grows within the scope, the current (largest) buffer is reclaimed to its
        // base so it can be reused, while the larger buffer itself is retained (released by the next Reset).
        [Test]
        public void GrowDuringScopeReclaimsCurrentBuffer()
        {
            var sba = new ScratchBufferAllocator();

            _ = sba.CreateArgSlice(16);
            var savedOffset = sba.ScratchBufferOffset;
            var totalBeforeGrow = sba.TotalLength;

            // Allocate more than the remaining capacity to force a grow (pushes the current buffer).
            _ = sba.CreateArgSlice(totalBeforeGrow);
            ClassicAssert.Greater(sba.TotalLength, totalBeforeGrow, "should have grown");
            var offsetAfterGrow = sba.ScratchBufferOffset;
            var totalAfterGrow = sba.TotalLength;

            sba.TryRewindToOffset(savedOffset);

            // The current buffer is reclaimed to its base, so the combined offset drops; the larger
            // buffer is retained (nothing freed yet).
            ClassicAssert.Less(sba.ScratchBufferOffset, offsetAfterGrow, "current buffer reclaimed");
            ClassicAssert.AreEqual(totalAfterGrow, sba.TotalLength, "larger buffer retained, nothing freed");

            // The reclaimed current buffer is reused by the next allocation rather than growing again.
            _ = sba.CreateArgSlice(totalBeforeGrow);
            ClassicAssert.AreEqual(totalAfterGrow, sba.TotalLength, "reused current buffer, no further grow");
        }

        // Rewinding back to the very first (default) state reclaims to zero but keeps the grown buffer.
        [Test]
        public void FirstAllocationFromDefaultRewindsToZeroAndRetainsBuffer()
        {
            var sba = new ScratchBufferAllocator();

            var savedOffset = sba.ScratchBufferOffset; // 0

            _ = sba.CreateArgSlice(64);
            ClassicAssert.AreEqual(64, sba.ScratchBufferOffset);

            sba.TryRewindToOffset(savedOffset);
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
                for (var i = 0; i < 8; i++)
                    _ = sba.CreateArgSlice(128);
                sba.TryRewindToOffset(savedOffset);
            }

            ClassicAssert.AreEqual(0, sba.ScratchBufferOffset);
            ClassicAssert.AreEqual(boundedTotal, sba.TotalLength, "retained memory stayed bounded across runs");
        }

        // Even when the first run grows the allocator across several buffers, repeated reserve+rewind
        // reuses the largest buffer instead of growing without bound across the batch. (Buffers stacked
        // during the first run's growth linger until the next Reset, so the combined offset settles at a
        // constant non-zero value rather than returning to 0.)
        [Test]
        public void RepeatedGrowingRunsReuseLargestBuffer()
        {
            var sba = new ScratchBufferAllocator();

            var totalAfterFirstRun = 0;
            var offsetAfterFirstRun = 0;
            for (var run = 0; run < 100; run++)
            {
                var savedOffset = sba.ScratchBufferOffset;
                for (var i = 0; i < 16; i++)
                    _ = sba.CreateArgSlice(256);
                sba.TryRewindToOffset(savedOffset);

                if (run == 0)
                {
                    totalAfterFirstRun = sba.TotalLength;
                    offsetAfterFirstRun = sba.ScratchBufferOffset;
                }
            }

            ClassicAssert.AreEqual(totalAfterFirstRun, sba.TotalLength, "did not grow beyond the first run");
            ClassicAssert.AreEqual(offsetAfterFirstRun, sba.ScratchBufferOffset, "offset did not accumulate across runs");
        }
    }
}