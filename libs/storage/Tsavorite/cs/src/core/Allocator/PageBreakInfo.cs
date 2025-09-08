// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// For direct writes of byte[] ("out of line") data (rather than from a page buffer), this information identifies overheads and final positions.
    /// It is also used by reads to calculate the total space required and fragments to be read.
    /// </summary>
    internal struct PageBreakInfo
    {
        /// <summary>
        /// The number of internal pages (complete pages of data, not including the first and last page).
        /// </summary>
        internal long internalPageCount;

        /// <summary>
        /// The total number of bytes taken by page headers and footers in the written span.
        /// </summary>
        internal readonly long TotalPageBreakBytes
        {
            get
            {
                var result = internalPageCount * (DiskPageHeader.Size + DiskPageFooter.Size);
                
                // We split first and last page fragments even if there are no page breaks, if the data length is longer than MaxCopySpanLen.
                // This isn't strictly a "page break" but it allows the caller to do a direct read/write of the larger sector-aligned portions, if desired.
                if (hasPageBreak)
                {
                    if (firstPageFragmentSize > 0)
                        result += DiskPageFooter.Size;
                    if (lastPageFragmentSize > 0)
                        result += DiskPageHeader.Size;
                }
                return result;
            }
        }

        /// <summary>
        /// The number of bytes in the data fragment that precedes the first page break. It is up to the caller whether to handle this specially
        /// (e.g. copy to buffer vs. write it directly from the input data if it's past some size such as <see cref="DiskStreamWriter.MaxCopySpanLen"/>).
        /// </summary>
        /// <remarks>This may be zero, if we're right at the end of the page when we call this.</remarks>
        internal int firstPageFragmentSize;

        /// <summary>
        /// The number of bytes in the data fragment that follows the last page break. It is up to the caller whether to handle this specially
        /// (e.g. copy to buffer vs. write it directly from the input data if it's past some size such as <see cref="DiskStreamWriter.MaxCopySpanLen"/>).
        /// </summary>
        /// <remarks>This may be zero, if the interior portion (including insertion of header and footer) ended on a sector boundary. And it end with the
        /// optionals, which may also cross a page boundary.</remarks>
        internal int lastPageFragmentSize;

        /// <summary>
        /// If true, there is at least one page break (even if there are no fully internal pages).
        /// </summary>
        internal bool hasPageBreak;

        /// <inheritdoc/>
        public override readonly string ToString()
            => $"#pages {internalPageCount}, breakBytes {TotalPageBreakBytes}, firstFragSize {firstPageFragmentSize}, lastFragSize {lastPageFragmentSize}";
    }
}