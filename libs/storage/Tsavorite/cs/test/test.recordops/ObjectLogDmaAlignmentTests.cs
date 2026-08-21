// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using NUnit.Framework;
using Tsavorite.core;

namespace Tsavorite.test.Objects
{
    /// <summary>
    /// Unit tests for the O_DIRECT alignment math used by the direct overflow DMA write (<see cref="ObjectLogDmaAlignment.Compute"/>):
    /// the source-alignment initial fragment and the disk-alignment header padding. Covers the initial-fragment cases the DMA write must
    /// handle: 0, sectorSize - 1, and sectorSize / 2.
    /// </summary>
    [TestFixture]
    internal class ObjectLogDmaAlignmentTests
    {
        // Post-condition helpers: after [header + headerPadding + sourceFragment], both the DMA source and the buffer position (== disk
        // offset, since the buffer base is sector-aligned) must be sector-aligned; both quantities are in [0, sectorSize).

        [Test]
        [Category("Smoke")]
        public void SourceFragmentZero([Values(512, 4096, 65536)] int sectorSize, [Values(0, 1, 8, 250, 4095)] int bufferHeaderPosition)
        {
            // Source already sector-aligned -> no initial fragment.
            var sourceAddress = (ulong)(3 * sectorSize);   // exact multiple
            ObjectLogDmaAlignment.Compute(sourceAddress, bufferHeaderPosition, sectorSize, out var sourceFragment, out var headerPadding);
            Assert.That(sourceFragment, Is.EqualTo(0));
            AssertAligned(sourceAddress, bufferHeaderPosition, sectorSize, sourceFragment, headerPadding);
        }

        [Test]
        [Category("Smoke")]
        public void SourceFragmentSectorSizeMinusOne([Values(512, 4096, 65536)] int sectorSize, [Values(0, 1, 8, 250, 4095)] int bufferHeaderPosition)
        {
            // Source is 1 byte past a boundary -> need sectorSize - 1 bytes to reach the next boundary.
            var sourceAddress = (ulong)(3 * sectorSize + 1);
            ObjectLogDmaAlignment.Compute(sourceAddress, bufferHeaderPosition, sectorSize, out var sourceFragment, out var headerPadding);
            Assert.That(sourceFragment, Is.EqualTo(sectorSize - 1));
            AssertAligned(sourceAddress, bufferHeaderPosition, sectorSize, sourceFragment, headerPadding);
        }

        [Test]
        [Category("Smoke")]
        public void SourceFragmentHalfSector([Values(512, 4096, 65536)] int sectorSize, [Values(0, 1, 8, 250, 4095)] int bufferHeaderPosition)
        {
            // Source is sectorSize/2 past a boundary -> need sectorSize/2 to reach the next boundary.
            var sourceAddress = (ulong)(3 * sectorSize + sectorSize / 2);
            ObjectLogDmaAlignment.Compute(sourceAddress, bufferHeaderPosition, sectorSize, out var sourceFragment, out var headerPadding);
            Assert.That(sourceFragment, Is.EqualTo(sectorSize / 2));
            AssertAligned(sourceAddress, bufferHeaderPosition, sectorSize, sourceFragment, headerPadding);
        }

        [Test]
        [Category("Smoke")]
        public void AlignmentHoldsForAllOffsets([Values(512, 4096)] int sectorSize)
        {
            // Exhaustively over a couple of sectors of source offset and header position, the post-conditions must hold.
            for (var srcOff = 0; srcOff < sectorSize; srcOff += (sectorSize / 32))
            {
                var sourceAddress = (ulong)(5 * sectorSize + srcOff);
                for (var hdrPos = 0; hdrPos < sectorSize; hdrPos += (sectorSize / 32))
                {
                    ObjectLogDmaAlignment.Compute(sourceAddress, hdrPos, sectorSize, out var sourceFragment, out var headerPadding);
                    AssertAligned(sourceAddress, hdrPos, sectorSize, sourceFragment, headerPadding);
                }
            }
        }

        static void AssertAligned(ulong sourceAddress, int bufferHeaderPosition, int sectorSize, int sourceFragment, int headerPadding)
        {
            Assert.That(sourceFragment, Is.InRange(0, sectorSize - 1), "sourceFragment must be in [0, sectorSize)");
            Assert.That(headerPadding, Is.InRange(0, sectorSize - 1), "headerPadding must be in [0, sectorSize)");

            // The DMA source (after the initial fragment) is sector-aligned.
            Assert.That((sourceAddress + (ulong)sourceFragment) % (ulong)sectorSize, Is.EqualTo(0UL), "DMA source not sector-aligned");

            // After [header + padding + fragment], the buffer position (== DMA disk offset) is sector-aligned.
            var afterFragment = bufferHeaderPosition + ChunkHeader.TotalSize + headerPadding + sourceFragment;
            Assert.That(afterFragment % sectorSize, Is.EqualTo(0), "DMA disk offset not sector-aligned");
        }
    }
}
