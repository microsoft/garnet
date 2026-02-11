// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Allure.NUnit;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    [AllureNUnit]
    [TestFixture]
    public class ScratchBufferAllocatorTests : AllureTestBase
    {
        [Test]

        public void CreateArgSliceAndRewindTest([Values(0, 127, 8192)] int maxInitialCapacity)
        {
            var string1 = new string('a', 5);
            var string2 = new string('b', 65);
            var string3 = new string('c', 6000);

            var sam = new ScratchBufferAllocator(maxInitialCapacity: maxInitialCapacity);

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
    }
}