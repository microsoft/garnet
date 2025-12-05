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
        /// Device used for main hybrid log
        /// </summary>
        public IDevice LogDevice;

        /// <summary>
        /// Device used for serialized heap objects in hybrid log
        /// </summary>
        public IDevice ObjectLogDevice;

        /// <summary>
        /// Size of a page, in bytes
        /// </summary>
        public long PageSize = 1 << 25;

        /// <summary>
        /// Size of a main log segment (group of pages), in bytes. Rounds down to power of 2.
        /// </summary>
        public long SegmentSize = 1L << 30;

        /// <summary>
        /// Size of an object log segment (group of pages), in bytes. Rounds down to power of 2.
        /// </summary>
        public long ObjSegmentSize = 1L << 30;

        /// <summary>
        /// Total size of in-memory part of log, in bytes. Rounds down to power of 2.
        /// </summary>
        public long MemorySize = 1L << 34;

        /// <summary>
        /// Controls how many pages should be empty to account for non-power-of-two-sized log
        /// </summary>
        public int MinEmptyPageCount = 0;

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
        /// Size of a read cache page, in bytes. Rounds down to power of 2.
        /// </summary>
        public long ReadCachePageSize = 1 << 25;

        /// <summary>
        /// Total size of read cache, in bytes. Rounds down to power of 2.
        /// </summary>
        public long ReadCacheMemorySize = 1L << 34;

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
        /// Maximum size of a key stored inline in the in-memory portion of the main log for both allocators.
        /// </summary>
        public int MaxInlineKeySize = 1 << LogSettings.kDefaultMaxInlineKeySizeBits;

        /// <summary>
        /// Maximum size of a value stored inline in the in-memory portion of the main log for <see cref="SpanByteAllocator{TStoreFunctions}"/>.
        /// </summary>
        public int MaxInlineValueSize = 1 << LogSettings.kDefaultMaxInlineValueSizeBits;

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
        /// <param name="logger"></param>
        public KVSettings(string baseDir, bool deleteDirOnDispose = false, ILoggerFactory loggerFactory = null, ILogger logger = null)
        {
            this.loggerFactory = loggerFactory;
            this.logger = logger;
            disposeDevices = true;
            this.deleteDirOnDispose = deleteDirOnDispose;
            this.baseDir = baseDir;

            LogDevice = baseDir == null ? new NullDevice() : Devices.CreateLogDevice(baseDir + "/hlog.log", deleteOnClose: deleteDirOnDispose);
            CheckpointDir = baseDir == null ? null : baseDir + "/checkpoints";
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
            var retStr = $"index: {Utility.PrettySize(IndexSize)}; log memory: {Utility.PrettySize(MemorySize)}; log page: {Utility.PrettySize(PageSize)}; log segment: {Utility.PrettySize(SegmentSize)}";
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

        internal LogSettings GetLogSettings()
        {
            return new LogSettings
            {
                ReadCopyOptions = ReadCopyOptions,
                LogDevice = LogDevice,
                ObjectLogDevice = ObjectLogDevice,
                MemorySizeBits = Utility.NumBitsPreviousPowerOf2(MemorySize),
                PageSizeBits = Utility.NumBitsPreviousPowerOf2(PageSize),
                SegmentSizeBits = Utility.NumBitsPreviousPowerOf2(SegmentSize),
                ObjectLogSegmentSizeBits = Utility.NumBitsPreviousPowerOf2(ObjSegmentSize),
                MutableFraction = MutableFraction,
                MinEmptyPageCount = MinEmptyPageCount,
                PreallocateLog = PreallocateLog,
                ReadCacheSettings = GetReadCacheSettings(),
                MaxInlineKeySizeBits = Utility.NumBitsPreviousPowerOf2(MaxInlineKeySize),
                MaxInlineValueSizeBits = Utility.NumBitsPreviousPowerOf2(MaxInlineValueSize)
            };
        }

        private ReadCacheSettings GetReadCacheSettings()
        {
            return ReadCacheEnabled ?
                new ReadCacheSettings
                {
                    MemorySizeBits = Utility.NumBitsPreviousPowerOf2(ReadCacheMemorySize),
                    PageSizeBits = Utility.NumBitsPreviousPowerOf2(ReadCachePageSize),
                    SecondChanceFraction = ReadCacheSecondChanceFraction
                }
                : null;
        }

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