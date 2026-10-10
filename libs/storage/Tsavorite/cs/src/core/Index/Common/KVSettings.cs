// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
    /// <summary>
    /// Configuration settings for hybrid log. Use Utility.ParseSize to specify sizes in familiar string notation (e.g., "4k" and "4 MB").
    /// </summary>
    public sealed class KVSettings : IDisposable
    {
        readonly bool disposeDevices = false;
        readonly bool deleteDirOnDispose = false;
        readonly string baseDir;

        /// <summary>
        /// Size of main hash index, in bytes. Rounds down to power of 2.
        /// </summary>
        public long IndexSize = 1L << 26;

        /// <summary>
        /// Ceiling on hash index overflow buckets, as a percentage of the main bucket count of the generation they
        /// belong to. Overflow buckets hold hash entries that do not fit the main bucket array, so they grow with the
        /// number of distinct keys, and are reclaimed only when the index grows. They chain linearly off a main bucket
        /// and are scanned by reads and upserts, so this percentage is the average chain length allowed before
        /// allocation throws: 300 permits three overflow buckets per main bucket. Zero selects
        /// <see cref="DefaultIndexOverflowThreshold"/>.
        /// </summary>
        /// <remarks>
        /// Expressed in the same unit as the index resize threshold so the two can be compared directly. A value at or
        /// below the resize threshold would be reached before the resize that reclaims overflow buckets.
        /// </remarks>
        public int IndexOverflowThreshold = DefaultIndexOverflowThreshold;

        /// <summary>
        /// Value used when <see cref="IndexOverflowThreshold"/> is zero.
        /// </summary>
        public const int DefaultIndexOverflowThreshold = 300;

        /// <summary>
        /// Largest ceiling the overflow-bucket page table can address, regardless of
        /// <see cref="IndexOverflowThreshold"/>.
        /// </summary>
        public static long MaxIndexOverflowMaxMemorySize => MallocFixedPageSize<HashBucket>.MaxMemorySizeLimit;

        /// <summary>
        /// Buckets the overflow allocator claims at construction. The allocation counter that drives index resize
        /// excludes these, so a caller comparing a resolved ceiling against a resize trigger must discount them.
        /// </summary>
        public static int OverflowBucketInitialAllocation => MallocFixedPageSize<HashBucket>.AllocateChunkSize;

        /// <summary>
        /// Size of one hash bucket, main or overflow, in bytes.
        /// </summary>
        public static int HashBucketSizeBytes => MallocFixedPageSize<HashBucket>.RecordSize;

        /// <summary>
        /// Memory an overflow-bucket generation commits as soon as it exists, whatever the threshold resolves to. The
        /// allocator commits whole pages and pre-allocates each page's successor, so a generation that has handed out
        /// nothing still holds this much.
        /// </summary>
        public static long OverflowBucketFloorMemorySize => MallocFixedPageSize<HashBucket>.MinMemorySize;

        /// <summary>
        /// Resolve a threshold to the overflow-bucket ceiling in bytes for an index generation of a given size,
        /// rounded down to a whole allocator page and clamped to what the page table can address.
        /// </summary>
        /// <param name="tableSizeBuckets">Main bucket count of the index generation.</param>
        /// <param name="overflowThreshold">Percentage of <paramref name="tableSizeBuckets"/>; zero selects
        /// <see cref="DefaultIndexOverflowThreshold"/>.</param>
        public static long GetIndexOverflowMaxMemorySize(long tableSizeBuckets, int overflowThreshold)
        {
            if (overflowThreshold <= 0)
                overflowThreshold = DefaultIndexOverflowThreshold;
            if (tableSizeBuckets <= 0)
                tableSizeBuckets = 1;

            // Scale before dividing, and floor at the smallest page table the allocator builds. GetLevelCount reads a
            // non-positive budget as "unconfigured" and substitutes its own much larger default.
            var requested = tableSizeBuckets * overflowThreshold * MallocFixedPageSize<HashBucket>.RecordSize / 100;
            if (requested < MallocFixedPageSize<HashBucket>.MinMemorySize)
                requested = MallocFixedPageSize<HashBucket>.MinMemorySize;
            return MallocFixedPageSize<HashBucket>.GetLevelCount(requested) * MallocFixedPageSize<HashBucket>.MemorySizeGranularity;
        }

        /// <summary>
        /// Device used for main hybrid log
        /// </summary>
        public IDevice LogDevice;

        /// <summary>
        /// Device used for serialized heap objects in hybrid log
        /// </summary>
        public IDevice ObjectLogDevice;

        /// <summary>
        /// Size of a main-log page, in bytes
        /// </summary>
        public long PageSize = 1 << 25;

        /// <summary>
        /// Main-log circular-buffer size if nonzero; rounds down to a power of 2 and errors if that does not allow PageSize.
        /// </summary>
        /// <remarks>
        /// If zero, calculate it from <see cref="LogMemorySize"/> and <see cref="PageSize"/>, which is also the max if both this and <see cref="LogMemorySize"/> are nonzero.
        /// </remarks>
        public int PageCount = 0;

        /// <summary>
        /// Size of a main log segment (group of pages), in bytes. Rounds down to power of 2.
        /// </summary>
        public long SegmentSize = 1L << 30;

        /// <summary>
        /// Size of an object log segment (group of pages), in bytes. Rounds down to power of 2.
        /// </summary>
        public long ObjectLogSegmentSize = 1L << 30;

        /// <summary>
        /// Total size of in-memory part of main log, in bytes. Rounds down to power of 2.
        /// </summary>
        public long LogMemorySize = 1L << 34;

        /// <summary>
        /// Fraction of log marked as mutable (in-place updates). Rounds down to power of 2.
        /// </summary>
        public double MutableFraction = 0.9;

        /// <summary>
        /// Control Read operations. These flags may be overridden by flags specified on session.NewSession or on the individual Read() operations
        /// </summary>
        public ReadCopyOptions ReadCopyOptions;

        /// <summary>
        /// Whether to preallocate the entire log (pages) in memory
        /// </summary>
        public bool PreallocateLog = false;

        /// <summary>
        /// Whether read cache is enabled
        /// </summary>
        public bool ReadCacheEnabled = false;

        /// <summary>
        /// Total size of readcache log if readcache is enabled, in bytes. Rounds down to power of 2.
        /// </summary>
        public long ReadCacheMemorySize = 1L << 32;

        /// <summary>
        /// Size of a read cache page, in bytes. Rounds down to power of 2.
        /// </summary>
        public long ReadCachePageSize = 1 << 25;

        /// <summary>
        /// Main-log circular-buffer size if nonzero; rounds down to a power of 2 and errors if that does not allow ReadCachePageSize.
        /// </summary>
        /// <remarks>
        /// If zero, calculate it from <see cref="ReadCacheMemorySize"/> and <see cref="ReadCachePageSize"/>, which is also the max if both this and <see cref="ReadCacheMemorySize"/> are nonzero.
        /// </remarks>
        public int ReadCachePageCount = 0;

        /// <summary>
        /// Fraction of log head (in memory) used for second chance copy to tail. This is (1 - MutableFraction) for the underlying log.
        /// </summary>
        public double ReadCacheSecondChanceFraction = 0.1;

        /// <summary>
        /// Checkpoint manager
        /// </summary>
        public ICheckpointManager CheckpointManager = null;

        /// <summary>
        /// Use specified directory for storing and retrieving checkpoints
        /// using local storage device.
        /// </summary>
        public string CheckpointDir = null;

        /// <summary>
        /// Whether Tsavorite should remove outdated checkpoints automatically
        /// </summary>
        public bool RemoveOutdatedCheckpoints = false;

        /// <summary>
        /// Try to recover from latest checkpoint, if available
        /// </summary>
        public bool TryRecoverLatest = false;

        /// <summary>
        /// Whether we should throttle the disk IO for checkpoints (one write at a time, wait between each write) and issue IO from separate task (-1 = throttling disabled)
        /// </summary>
        public int ThrottleCheckpointFlushDelayMs = -1;

        /// <summary>
        /// Settings for recycling deleted records on the log.
        /// </summary>
        public RevivificationSettings RevivificationSettings;

        /// <summary>
        /// Epoch instance used by the store
        /// </summary>
        public LightEpoch Epoch = null;

        /// <summary>
        /// State machine driver for the store
        /// </summary>
        public StateMachineDriver StateMachineDriver = null;

        /// <summary>
        /// Default maximum size of a key stored inline in the in-memory portion of the main log; this is the maximum inline size for <see cref="RecordDataHeader.KeyLength"/>.
        /// </summary>
        public const int DefaultMaxInlineKeySize = 1022;

        /// <summary>
        /// Maximum size of a key stored inline in the in-memory portion of the main log.
        /// </summary>
        public int MaxInlineKeySize = DefaultMaxInlineKeySize;

        /// <summary>
        /// Default maximum size of a value stored inline in the in-memory portion of the main log for both allocators; this is less than the maximum inline size for <see cref="RecordDataHeader.ValueLength"/>.
        /// </summary>
        public const int DefaultMaxInlineValueSize = 1024 * 1024;

        /// <summary>
        /// Maximum size of a value stored inline in the in-memory portion of the main log for <see cref="SpanByteAllocator{TStoreFunctions}"/>.
        /// </summary>
        public int MaxInlineValueSize = DefaultMaxInlineValueSize;

        /// <summary>Sentinel value indicating that the default <see cref="IStreamBuffer.DefaultInitialIORecordSize"/> should be used.</summary>
        public const int UseDefaultInitialIORecordSize = -1;

        /// <summary>
        /// Initial IO size for reading records from disk. <see cref="UseDefaultInitialIORecordSize"/> means unset;
        /// the resolution chain (per-operation → session → store → <see cref="IStreamBuffer.DefaultInitialIORecordSize"/>) determines the actual value.
        /// </summary>
        public int InitialIORecordSize = UseDefaultInitialIORecordSize;

        /// <summary>
        /// Create default configuration settings for TsavoriteKV. You need to create and specify LogDevice 
        /// explicitly with this API.
        /// Use Utility.ParseSize to specify sizes in familiar string notation (e.g., "4k" and "4 MB").
        /// Default index size is 64MB.
        /// </summary>
        public KVSettings() { }

        public ILoggerFactory loggerFactory;
        public ILogger logger;

        /// <summary>
        /// Create default configuration backed by local storage at given base directory.
        /// Use Utility.ParseSize to specify sizes in familiar string notation (e.g., "4k" and "4 MB").
        /// Default index size is 64MB.
        /// </summary>
        /// <param name="baseDir">Base directory (without trailing path separator)</param>
        /// <param name="deleteDirOnDispose">Whether to delete base directory on dispose. This option prevents later recovery.</param>
        /// <param name="loggerFactory"></param>
        /// <param name="logger"></param>
        public KVSettings(string baseDir, bool deleteDirOnDispose = false, ILoggerFactory loggerFactory = null, ILogger logger = null)
        {
            this.loggerFactory = loggerFactory;
            this.logger = logger;
            disposeDevices = true;
            this.deleteDirOnDispose = deleteDirOnDispose;
            this.baseDir = baseDir;

            LogDevice = baseDir == null ? new NullDevice() : Devices.CreateLogDevice(Path.Combine(baseDir, "hlog.log"), deleteOnClose: deleteDirOnDispose);
            CheckpointDir = baseDir == null ? null : Path.Combine(baseDir, "checkpoints");
        }

        /// <inheritdoc />
        public void Dispose()
        {
            if (disposeDevices)
            {
                LogDevice?.Dispose();
                ObjectLogDevice?.Dispose();
                if (deleteDirOnDispose && baseDir != null)
                {
                    try { new DirectoryInfo(baseDir).Delete(true); } catch { }
                }
            }
        }

        /// <inheritdoc />
        public override string ToString()
        {
            var retStr = $"index: {Utility.PrettySize(IndexSize)}; overflow max: {GetOverflowThreshold()}% of index ({Utility.PrettySize(GetIndexOverflowMaxMemorySize(GetIndexSizeCacheLines(), IndexOverflowThreshold))}); log memory: {Utility.PrettySize(LogMemorySize)}; log page: {Utility.PrettySize(PageSize)}; log segment: {Utility.PrettySize(SegmentSize)}";
            retStr += $"; log device: {(LogDevice == null ? "null" : LogDevice.GetType().Name)}";
            retStr += $"; obj log device: {(ObjectLogDevice == null ? "null" : ObjectLogDevice.GetType().Name)}";
            retStr += $"; mutable fraction: {MutableFraction};";
            retStr += $"; read cache (rc): {(ReadCacheEnabled ? "yes" : "no")}";
            retStr += $"; read copy options: {ReadCopyOptions}";
            if (ReadCacheEnabled)
                retStr += $"; rc memory: {Utility.PrettySize(ReadCacheMemorySize)}; rc page: {Utility.PrettySize(ReadCachePageSize)}";
            return retStr;
        }

        internal long GetIndexSizeCacheLines()
        {
            long adjustedSize = Utility.PreviousPowerOf2(IndexSize);
            if (adjustedSize < 64)
                throw new TsavoriteException($"{nameof(IndexSize)} should be at least of size 1 cache line (64 bytes)");
            if (IndexSize != adjustedSize)  // Don't use string interpolation when logging messages because it makes it impossible to group by the message template.
                logger?.LogInformation("Warning: using lower value {0} instead of specified {1} for {2}", adjustedSize, IndexSize, nameof(IndexSize));
            return adjustedSize / 64;
        }

        internal static long SetIndexSizeFromCacheLines(long cacheLines)
            => cacheLines * 64;

        /// <summary>
        /// Resolve <see cref="IndexOverflowThreshold"/>, substituting the default when it is unset.
        /// </summary>
        internal int GetOverflowThreshold()
            => IndexOverflowThreshold > 0 ? IndexOverflowThreshold : DefaultIndexOverflowThreshold;

        internal LogSettings GetLogSettings()
            => new()
            {
                ReadCopyOptions = ReadCopyOptions,
                LogDevice = LogDevice,
                ObjectLogDevice = ObjectLogDevice,
                MemorySize = LogMemorySize,
                PageSizeBits = Utility.NumBitsPreviousPowerOf2(PageSize),
                PageCount = PageCount,
                SegmentSizeBits = Utility.NumBitsPreviousPowerOf2(SegmentSize),
                ObjectLogSegmentSizeBits = Utility.NumBitsPreviousPowerOf2(ObjectLogSegmentSize),
                MutableFraction = MutableFraction,
                PreallocateLog = PreallocateLog,
                ReadCacheSettings = GetReadCacheSettings(),
                MaxInlineKeySize = MaxInlineKeySize,
                MaxInlineValueSize = MaxInlineValueSize
            };

        private ReadCacheSettings GetReadCacheSettings()
            => ReadCacheEnabled ?
                new()
                {
                    MemorySize = ReadCacheMemorySize,
                    PageSizeBits = Utility.NumBitsPreviousPowerOf2(ReadCachePageSize),
                    PageCount = ReadCachePageCount,
                    SecondChanceFraction = ReadCacheSecondChanceFraction
                }
                : null;

        internal CheckpointSettings GetCheckpointSettings()
        {
            return new CheckpointSettings
            {
                CheckpointDir = CheckpointDir,
                CheckpointManager = CheckpointManager,
                RemoveOutdated = RemoveOutdatedCheckpoints,
                ThrottleCheckpointFlushDelayMs = ThrottleCheckpointFlushDelayMs
            };
        }
    }
}