// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.IO;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;

namespace Tsavorite.test
{
    /// <summary>
    /// Unit tests for <see cref="TsavoriteLog"/>'s partial (page-tail) allocation, <c>AllocateBlockPartial</c>. When an
    /// allocation would run off the end of a page it either <b>splits</b> — granting the page tail as a partial allocation
    /// (returned length &lt; numSlots) so the caller can continue on the next page — or, when the tail is too small to leave
    /// both parts at least <c>partialSlots</c>, <b>crosses</b> entirely to the next page (returned length == numSlots).
    /// </summary>
    [TestFixture]
    internal class AllocateBlockPartialTests : TestBase
    {
        private TsavoriteLog log;
        private IDevice device;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            device = Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "AllocateBlockPartial.log"), deleteOnClose: true);
            // Small power-of-2 pages so the tail can be positioned cheaply; ample memory so crossing pages never evicts/flushes.
            log = new TsavoriteLog(new TsavoriteLogSettings { LogDevice = device, PageSizeBits = 12, MemorySizeBits = 20, SegmentSizeBits = 22, LogCommitDir = TestUtils.MethodTestDir });
        }

        [TearDown]
        public void TearDown()
        {
            log?.Dispose();
            log = null;
            device?.Dispose();
            device = null;
            TestUtils.OnTearDown();
        }

        [Test]
        [Category("TsavoriteLog")]
        public void AllocateBlockPartialSplitVsCrossPageTest()
        {
            var pageSizeBits = log.UnsafeGetLogPageSizeBits();
            var pageSize = 1 << pageSizeBits;

            // TsavoriteLog logical addresses pack as (page &lt;&lt; pageSizeBits) | offset, and the log has no page header.
            long PageOf(long address) => address >> pageSizeBits;
            int OffsetOf(long address) => (int)(address & (pageSize - 1));

            // Advance the tail so exactly `remainder` bytes are left on the current page (partialSlots 0 => the fill never splits).
            void PositionToLeave(int remainder)
            {
                var offset = OffsetOf(log.TailAddress);
                var target = pageSize - remainder;
                ClassicAssert.LessOrEqual(offset, target, "not enough room on the current page to leave the requested remainder");
                if (offset < target)
                    _ = log.AllocateBlockPartialForTest(target - offset, partialSlots: 0, out _);
            }

            const int numSlots = 2048;      // half a page
            const int partialSlots = 512;   // 1/8 page: the minimum size each half of a split must have

            // --- Split: leave a remainder in [partialSlots, numSlots - partialSlots] so both halves are large enough to split. ---
            const int splitRemainder = 1024;    // 512 <= 1024 <= 2048 - 512
            PositionToLeave(splitRemainder);
            var tailBeforeSplit = log.TailAddress;
            ClassicAssert.AreEqual(pageSize - splitRemainder, OffsetOf(tailBeforeSplit));

            var splitAddress = log.AllocateBlockPartialForTest(numSlots, partialSlots, out var splitLength);
            ClassicAssert.AreEqual(splitRemainder, splitLength, "a split grants only the page-tail remainder (< numSlots)");
            ClassicAssert.AreEqual(PageOf(tailBeforeSplit), PageOf(splitAddress), "a split stays on the current page");
            ClassicAssert.AreEqual(OffsetOf(tailBeforeSplit), OffsetOf(splitAddress), "a split starts at the previous tail");

            // A split resets the tail to the end of the page, so subsequent allocations begin on the next page.

            // --- Cross: leave a remainder smaller than partialSlots so the whole allocation moves to the next page. ---
            const int crossRemainder = 256;     // < partialSlots
            PositionToLeave(crossRemainder);
            var tailBeforeCross = log.TailAddress;
            ClassicAssert.AreEqual(pageSize - crossRemainder, OffsetOf(tailBeforeCross));

            var crossAddress = log.AllocateBlockPartialForTest(numSlots, partialSlots, out var crossLength);
            ClassicAssert.AreEqual(numSlots, crossLength, "a cross grants the full request on the next page");
            ClassicAssert.AreEqual(PageOf(tailBeforeCross) + 1, PageOf(crossAddress), "a cross moves to the next page");
            ClassicAssert.AreEqual(0, OffsetOf(crossAddress), "a cross starts at the beginning of the next page");
        }
    }
}