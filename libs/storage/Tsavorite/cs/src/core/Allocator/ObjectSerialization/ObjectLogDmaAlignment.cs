// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// O_DIRECT alignment math for a direct overflow DMA write (see <see cref="ObjectLogWriter{TStoreFunctions}"/>). On-disk layout of a
    /// DMA'd overflow is <c>[ChunkHeader][alignmentPadding][data]</c>; the sector-aligned interior of the data is DMA'd straight from the
    /// pinned byte[], which requires the DMA <b>source</b> address, <b>disk offset</b>, and <b>length</b> to all be sector-aligned.
    /// </summary>
    internal static class ObjectLogDmaAlignment
    {
        /// <summary>Compute the two alignment quantities for a direct overflow DMA write:
        /// <list type="bullet">
        ///   <item><paramref name="sourceFragment"/> — the count of leading data bytes to copy through the buffer so the DMA source
        ///     (<paramref name="sourceDataAddress"/> + <paramref name="sourceFragment"/>) reaches a sector boundary.</item>
        ///   <item><paramref name="headerPadding"/> — zero bytes written after the 8-byte <see cref="ChunkHeader"/> so that, after
        ///     [header + padding + fragment], the buffer write position (<paramref name="bufferHeaderPosition"/> is where the header starts)
        ///     — and thus the DMA disk offset — is sector-aligned.</item>
        /// </list>
        /// Both results are in <c>[0, sectorSize)</c>. The buffer base is sector-aligned, so buffer-position alignment equals disk-offset
        /// alignment.</summary>
        internal static void Compute(ulong sourceDataAddress, int bufferHeaderPosition, int sectorSize, out int sourceFragment, out int headerPadding)
        {
            sourceFragment = (int)(((ulong)sectorSize - (sourceDataAddress % (ulong)sectorSize)) % (ulong)sectorSize);
            headerPadding = (sectorSize - ((bufferHeaderPosition + ChunkHeader.TotalSize + sourceFragment) % sectorSize)) % sectorSize;
        }
    }
}
