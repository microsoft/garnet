// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma warning disable IDE1006 // Naming Styles

namespace Tsavorite.core
{
    /// <summary>
    /// Configuration settings for hybrid log
    /// </summary>
    internal class LogSettings
    {
        /// <summary>Minimum number of bits for a page size</summary>
        public const int kMinPageSizeBits = 6;
        /// <summary>Maximum number of bits for a page size</summary>
        public const int kMaxPageSizeBits = 30;

        /// <summary>Minimum number of bits for a main-log segment (segments consist of one or more pages)</summary>
        public const int kMinMainLogSegmentSizeBits = kMinPageSizeBits;
        /// <summary>Minimum number of bits for a segment (segments consist of one or more pages). This minimum size is also the size of the <see cref="DiskWriteBuffer"/> buffer,
        ///     so the segment must be a multiple of this (which is guaranteed as both are powers of 2, as long as this minimum is observed).</summary>
        /// <remarks>During flush we may create multiple buffers, depending on the degree of parallelism allowed by page concurrency and <see cref="NumberOfFlushBuffers"/>.</remarks>
        public const int kMinObjectLogSegmentSizeBits = 22; // 4MB
        /// <summary>Maximum number of bits for a main-log or object-log segment (segments consist of one or more pages). This is also the size of the read/write buffers
        ///     for object serialization to the object log.</summary>
        public const int kMaxSegmentSizeBits = 62;

        /// <summary>Minimum number of bits for the size of the in-memory portion of the log</summary>
        public const int kMinMemorySizeBits = kMinPageSizeBits;
        /// <summary>Maximum number of bits for the size of the in-memory portion of the log</summary>
        public const int kMaxMemorySizeBits = kMaxSegmentSizeBits;

        /// <summary>Minimum <see cref="NumberOfFlushBuffers"/> per flush operation. Must be a power of 2</summary>
        public const int kMinFlushBuffers = 2;
        /// <summary>Maximum <see cref="NumberOfFlushBuffers"/> per flush operation. Must be a power of 2</summary>
        public const int kMaxFlushBuffers = 64;

        /// <summary>Minimum <see cref="NumberOfFlushBuffers"/> per flush operation. Must be a power of 2</summary>
        public const int kMinDeserializationBuffers = 2;
        /// <summary>Maximum <see cref="NumberOfFlushBuffers"/> per flush operation. Must be a power of 2</summary>
        public const int kMaxDeserializationBuffers = 64;

        /// <summary>Default number of bits for the size of an inline (not overflow) key</summary>
        public const int kDefaultMaxInlineKeySizeBits = kLowestMaxInlineSizeBits + 1;

        /// <summary>Max inline key size is 1 byte for the length (0 or 1, with 1 added to make a range of 1-2), so that the in-memory varbyte indicator word is &lt;= sizeof(long) for atomic assignment.</summary>
        public const int kMaxInlineKeySize = 1 << 16;           // 64KB

        /// <summary>Default number of bits for the size of an inline (not overflow) value, for <see cref="SpanByteAllocator{TStoreFunctions}"/></summary>
        public const int kDefaultMaxInlineValueSizeBits = 12;   // 4KB

        /// <summary>Max inline value size is 2 bytes for the length (0 to 3, with 1 added to make a range of 1-4, and we max at 3), so that the in-memory varbyte indicator word is &lt;= sizeof(long) for atomic assignment.</summary>
        public const int kMaxInlineValueSize = 1 << 24;         // 16MB

        /// <summary>Minimum number of bits for the size of an overflow (int inline) key or value</summary>
        public const int kLowestMaxInlineSizeBits = kMinPageSizeBits;

        /// <summary>Maximum size of a string is 512MB</summary>
        public const int kMaxStringSizeBits = 29;

        /// <summary>
        /// Device used for main hybrid log
        /// </summary>
        public IDevice LogDevice;

        /// <summary>
        /// Device used for serialized heap objects in hybrid log
        /// </summary>
        public IDevice ObjectLogDevice;

        /// <summary>
        /// Size of a segment (group of pages), in bits
        /// </summary>
        public int PageSizeBits = 25;

        /// <summary>
        /// Size of a segment (group of pages) in the main log, in bits
        /// </summary>
        public int SegmentSizeBits = 30;

        /// <summary>
        /// Size of a segment (group of pages) in the object log, in bits
        /// </summary>
        public int ObjectLogSegmentSizeBits = 40;

        /// <summary>
        /// Total size of in-memory part of log, in bits
        /// </summary>
        public int MemorySizeBits = 34;

        /// <summary>
        /// Controls how many pages should be empty to account for non-power-of-two-sized log
        /// </summary>
        public int MinEmptyPageCount = 0;

        /// <summary>
        /// Fraction of log marked as mutable (in-place updates)
        /// </summary>
        public double MutableFraction = 0.9;

        /// <summary>
        /// Control Read copy operations. These values may be overridden by flags specified on session.NewSession or on the individual Read() operations
        /// </summary>
        public ReadCopyOptions ReadCopyOptions;

        /// <summary>
        /// Settings for optional read cache. Overrides the "copy reads to tail" setting.
        /// </summary>
        public ReadCacheSettings ReadCacheSettings = null;

        /// <summary>
        /// Whether to preallocate the entire log (pages) in memory
        /// </summary>
        public bool PreallocateLog = false;

        /// <summary>
        /// Maximum size of a key stored inline in the in-memory portion of the main log for both allocators.
        /// </summary>
        public int MaxInlineKeySizeBits = kDefaultMaxInlineKeySizeBits;

        /// <summary>
        /// Maximum size of a value stored inline in the in-memory portion of the main log for <see cref="SpanByteAllocator{TStoreFunctions}"/>.
        /// </summary>
        public int MaxInlineValueSizeBits = kDefaultMaxInlineValueSizeBits;

        /// <summary>
        /// Number of page buffers during a Flush operation on a page or portion of a page. There may be multiple sets of buffers at any given time,
        /// depending on page parallelism. Must be a power of 2.
        /// </summary>
        /// <remarks>Validated for all allocators, but only used by <see cref="ObjectAllocator{TStoreFunctions}"/>.</remarks>
        public int NumberOfFlushBuffers = 4;

        /// <summary>
        /// Number of page buffers during a Flush operation on a page or portion of a page. There may be multiple sets of buffers at any given time,
        /// depending on page parallelism. Must be a power of 2.
        /// </summary>
        /// <remarks>Validated for all allocators, but only used by <see cref="ObjectAllocator{TStoreFunctions}"/>.</remarks>
        public int NumberOfDeserializationBuffers = 4;
    }
}