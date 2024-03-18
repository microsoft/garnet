// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Tsavorite.core
{
    /// <summary>
    /// Configuration settings for serializing objects
    /// </summary>
    /// <typeparam name="Key"></typeparam>
    /// <typeparam name="Value"></typeparam>
    public class SerializerSettings<Key, Value>
    {
        /// <summary>
        /// Key serializer
        /// </summary>
        public Func<IObjectSerializer<Key>> keySerializer;

        /// <summary>
        /// Value serializer
        /// </summary>
        public Func<IObjectSerializer<Value>> valueSerializer;
    }

    /// <summary>
    /// Configuration settings for hybrid log
    /// </summary>
    public class LogSettings
    {
        /// <summary>Minimum number of bits for a page size</summary>
        public const int kMinPageSizeBits = 6;
        /// <summary>Maximum number of bits for a page size</summary>
        public const int kMaxPageSizeBits = 30;

        /// <summary>Minimum number of bits for a segment (segments consist of one or more pages)</summary>
        public const int kMinSegmentSizeBits = kMinPageSizeBits;
        /// <summary>Maximum number of bits for a page size (segments consist of one or more pages)</summary>
        public const int kMaxSegmentSizeBits = 62;

        /// <summary>Minimum number of bits for the size of the in-memory portion of the log</summary>
        public const int kMinMemorySizeBits = kMinSegmentSizeBits;
        /// <summary>Maximum number of bits for the size of the in-memory portion of the log</summary>
        public const int kMaxMemorySizeBits = kMaxSegmentSizeBits;

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
        /// Size of a segment (group of pages), in bits
        /// </summary>
        public int SegmentSizeBits = 30;

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
        /// Settings for optional read cache
        /// Overrides the "copy reads to tail" setting
        /// </summary>
        public ReadCacheSettings ReadCacheSettings = null;

        /// <summary>
        /// Whether to preallocate the entire log (pages) in memory
        /// </summary>
        public bool PreallocateLog = false;
    }

    /// <summary>
    /// Configuration settings for hybrid log
    /// </summary>
    public class ReadCacheSettings
    {
        /// <summary>
        /// Size of a segment (group of pages), in bits
        /// </summary>
        public int PageSizeBits = 25;

        /// <summary>
        /// Total size of in-memory part of log, in bits
        /// </summary>
        public int MemorySizeBits = 34;

        /// <summary>
        /// Fraction of log head (in memory) used for second chance 
        /// copy to tail. This is (1 - MutableFraction) for the 
        /// underlying log
        /// </summary>
        public double SecondChanceFraction = 0.1;
    }
}