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
        public const int kMinPageSizeBits = 12; // 4KB
        /// <summary>Maximum number of bits for a page size</summary>
        public const int kMaxPageSizeBits = 27; // 128MB

        /// <summary>Minimum number of bits for a main-log segment (segments consist of one or more pages)</summary>
        public const int kMinMainLogSegmentSizeBits = kMinPageSizeBits;
        /// <summary>Minimum number of bits for a segment (segments consist of one or more pages). This minimum size is also the size of the <see cref="DiskWriteBuffer"/> buffer,
        ///     so the segment must be a multiple of this (which is guaranteed as both are powers of 2, as long as this minimum is observed).</summary>
        /// <remarks>During flush we may create multiple buffers, depending on the degree of parallelism allowed by page concurrency and <see cref="NumberOfFlushBuffers"/>.</remarks>
        public const int kMinObjectLogSegmentSizeBits = 22; // 4MB
        /// <summary>Maximum number of bits for a main-log or object-log segment (segments consist of one or more pages). This is also the size of the read/write buffers
        ///     for object serialization to the object log.</summary>
        public const int kMaxSegmentSizeBits = 62;

        public const int kMinPageCount = 2;

        /// <summary>Minimum number of bits for the size of the in-memory portion of the log</summary>
        public const int kMinMemorySizeBits = kMinPageSizeBits + 1;
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

        /// <summary>Maximum size of a string (key or value) is 512MB</summary>
        public const int kMaxStringSizeBits = 29;                                       // 512MB

        /// <summary>Default number of bits for the size of an inline (not overflow) key</summary>
        public const int DefaultMaxInlineKeySize = 1 << (kLowestMaxInlineSizeBits + 1); // 128B

        /// <summary>Default number of bits for the size of an inline (not overflow) value, for <see cref="SpanByteAllocator{TStoreFunctions}"/></summary>
        public const int DefaultMaxInlineValueSize = 1 << 12;                           // 4KB

        /// <summary>Minimum number of bits for the size of an overflow (int inline) key or value</summary>
        public const int kLowestMaxInlineSizeBits = 6;                                  // 64B

        /// <summary>Maximum inline key size that fits in the RDH KeyLength field.
        /// <para>Set to one less than the field's max value (`(1 &lt;&lt; <see cref="RecordDataHeader.kKeyLengthBits"/>) - 1`), so inline keys occupy
        /// 0..MaxInlineKeySizeLimit. For overflow keys (R11), the low <see cref="RecordDataHeader.kKeyLengthBits"/> bits of the on-disk length
        /// are stored in the RDH field and can be any value 0..fieldMask; the dependency `MaxInlineKeySizeLimit == fieldMask - 1`
        /// ensures the masked-low-N-bits encoding has a consistent contract with the inline-size validation in KVSettings.</para></summary>
        public const int MaxInlineKeySizeLimit = (1 << RecordDataHeader.kKeyLengthBits) - 2;

        /// <summary>Maximum inline value size that fits in the RDH ValueLength field.
        /// <para>Set to one less than the field's max value (`(1 &lt;&lt; <see cref="RecordDataHeader.kValueLengthBits"/>) - 1`), so inline values
        /// occupy 0..MaxInlineValueSizeLimit. For overflow / object values (R11), the low <see cref="RecordDataHeader.kValueLengthBits"/> bits
        /// of the on-disk length are stored in the RDH field and can be any value 0..fieldMask; the dependency `MaxInlineValueSizeLimit ==
        /// fieldMask - 1` ensures the masked-low-N-bits encoding has a consistent contract with the inline-size validation in KVSettings.</para></summary>
        public const int MaxInlineValueSizeLimit = (1 << RecordDataHeader.kValueLengthBits) - 2;

        /// <summary>Minimum allowed <see cref="MaxInlineKeySize"/> / <see cref="MaxInlineValueSize"/>.</summary>
        public const int MinMaxInlineSize = 1 << 6;                                     // 64B

        /// <summary>Whether the given key size exceeds the RDH KeyLength inline limit and requires overflow encoding.</summary>
        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        public static bool KeySizeExceedsHeaderLimit(int size) => size > MaxInlineKeySizeLimit;

        /// <summary>Whether the given value size exceeds the RDH ValueLength inline limit and requires overflow encoding.</summary>
        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        public static bool ValueSizeExceedsHeaderLimit(long size) => size > MaxInlineValueSizeLimit;

        /// <summary>
        /// Device used for main hybrid log
        /// </summary>
        public IDevice LogDevice;

        /// <summary>
        /// Device used for serialized heap objects in hybrid log
        /// </summary>
        public IDevice ObjectLogDevice;

        /// <summary>
        /// Total size of in-memory part of log, in bytes. Does not need to be a power of 2
        /// </summary>
        public long MemorySize = 1L << 34;

        /// <summary>
        /// Size of a page in bits
        /// </summary>
        public int PageSizeBits = 25;

        /// <summary>
        /// Number of pages in the circular buffer, rounded down to nearest power of 2.
        /// </summary>
        /// <remarks>If 0, it is calculated from <see cref="MemorySize"/> and <see cref="PageSizeBits"/>, which is also the max if both this and <see cref="MemorySize"/> are nonzero.</remarks>
        public int PageCount = 0;

        /// <summary>
        /// Size of a segment (group of pages) in the main log, in bits
        /// </summary>
        public int SegmentSizeBits = 30;    // 1GB

        /// <summary>
        /// Size of a segment (group of pages) in the object log, in bits
        /// </summary>
        public int ObjectLogSegmentSizeBits = 33;   // 8GB

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
        public int MaxInlineKeySize = DefaultMaxInlineKeySize;

        /// <summary>
        /// Maximum size of a value stored inline in the in-memory portion of the main log for <see cref="SpanByteAllocator{TStoreFunctions}"/>.
        /// </summary>
        public int MaxInlineValueSize = DefaultMaxInlineValueSize;

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