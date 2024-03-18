// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;

namespace Tsavorite.core
{
    /// <summary>
    /// Delegate for getting memory from user
    /// </summary>
    /// <param name="minLength">Minimum length of returned byte array</param>
    /// <returns></returns>
    public delegate byte[] GetMemory(int minLength);

    /// <summary>
    /// Type of checksum to add to log
    /// </summary>
    public enum LogChecksumType
    {
        /// <summary>
        /// No checksums
        /// </summary>
        None,
        /// <summary>
        /// Checksum per entry
        /// </summary>
        PerEntry
    }

    /// <summary>
    /// Tsavorite Log Settings
    /// </summary>
    public class TsavoriteLogSettings : IDisposable
    {
        readonly bool disposeDevices = false;
        readonly bool deleteDirOnDispose = false;
        readonly string baseDir;

        /// <summary>
        /// Device used for log
        /// </summary>
        public IDevice LogDevice = new NullDevice();

        /// <summary>
        /// Size of a page, in bits
        /// </summary>
        public long PageSize = 1L << 22;

        /// <summary>
        /// Support bit-based setting of page size for backward compatibility, use PageSize directly for simplicity.
        /// </summary>
        public int PageSizeBits { set { PageSize = 1L << value; } }

        /// <summary>
        /// Total size of in-memory part of log, in bits
        /// Should be at least one page long
        /// Num pages = 2^(MemorySizeBits-PageSizeBits)
        /// </summary>
        public long MemorySize = 1L << 23;

        /// <summary>
        /// Support bit-based setting of memory size for backward compatibility, use MemorySize directly for simplicity.
        /// </summary>
        public int MemorySizeBits { set { MemorySize = 1L << value; } }

        /// <summary>
        /// Size of a segment (group of pages), in bits
        /// This is the granularity of files on disk
        /// </summary>
        public long SegmentSize = 1L << 30;

        /// <summary>
        /// Support bit-based setting of segment size for backward compatibility, use SegmentSize directly for simplicity.
        /// </summary>
        public int SegmentSizeBits { set { SegmentSize = 1L << value; } }

        /// <summary>
        /// Log commit manager - if you want to override the default implementation of commit.
        /// </summary>
        public ILogCommitManager LogCommitManager = null;

        /// <summary>
        /// Use specified directory (path) as base for storing and retrieving log commits. By default,
        /// commits will be stored in a folder named log-commits under this directory. If not provided, 
        /// we use the base path of the log device by default.
        /// </summary>
        public string LogCommitDir = null;

        /// <summary>
        /// User callback to allocate memory for read entries
        /// </summary>
        public GetMemory GetMemory = null;

        /// <summary>
        /// Type of checksum to add to log
        /// </summary>
        public LogChecksumType LogChecksum = LogChecksumType.None;

        /// <summary>
        /// Fraction of log marked as mutable (uncommitted)
        /// </summary>
        public double MutableFraction = 0;

        /// <summary>
        /// Use TsavoriteLog as read-only iterator/viewer of log being committed by another instance
        /// </summary>
        public bool ReadOnlyMode = false;

        /// <summary>
        /// When FastCommitMode is enabled, TsavoriteLog will reduce commit critical path latency, but may result in slower
        /// recovery to a commit on restart. Additionally, FastCommitMode is only possible when log checksum is turned
        /// on.
        /// </summary>
        public bool FastCommitMode = false;

        /// <summary>
        /// When true, we automatically delete commit files that are covered by a successful subsequent commit, and during
        /// recovery we delete all commit files other than the one we have recovered to.
        /// </summary>
        public bool RemoveOutdatedCommits = true;

        /// <summary>
        /// Log commit policy that influences the behavior of Commit() calls.
        /// </summary>
        public LogCommitPolicy LogCommitPolicy = LogCommitPolicy.Default();

        /// <summary>
        /// Try to recover from latest commit, if available
        /// </summary>
        public bool TryRecoverLatest = true;

        /// <summary>
        /// Whether we refresh safe tail address as records are inserted
        /// </summary>
        public bool AutoRefreshSafeTailAddress = false;

        /// <summary>
        /// Whether we automatically commit the log as records are inserted
        /// </summary>
        public bool AutoCommit = false;

        /// <summary>
        /// Create default configuration settings for TsavoriteLog. You need to create and specify LogDevice 
        /// explicitly with this API.
        /// Use Utility.ParseSize to specify sizes in familiar string notation (e.g., "4k" and "4 MB").
        /// </summary>
        public TsavoriteLogSettings() { }

        /// <summary>
        /// Create default configuration backed by local storage at given base directory.
        /// Use Utility.ParseSize to specify sizes in familiar string notation (e.g., "4k" and "4 MB").
        /// Default index size is 64MB.
        /// </summary>
        /// <param name="baseDir">Base directory (without trailing path separator)</param>
        /// <param name="deleteDirOnDispose">Whether to delete base directory on dispose. This option prevents later recovery.</param>
        public TsavoriteLogSettings(string baseDir, bool deleteDirOnDispose = false)
        {
            disposeDevices = true;
            this.deleteDirOnDispose = deleteDirOnDispose;
            this.baseDir = baseDir;
            LogDevice = baseDir == null ? new NullDevice() : Devices.CreateLogDevice(baseDir + "/tsavoritelog.log", deleteOnClose: deleteDirOnDispose);
            LogCommitDir = baseDir;
        }

        /// <inheritdoc />
        public void Dispose()
        {
            if (disposeDevices)
            {
                LogDevice?.Dispose();
                if (deleteDirOnDispose && baseDir != null)
                {
                    try { new DirectoryInfo(baseDir).Delete(true); } catch { }
                }
            }
        }

        /// <inheritdoc />
        public override string ToString()
        {
            var retStr = $"log memory: {Utility.PrettySize(MemorySize)}; log page: {Utility.PrettySize(PageSize)}; log segment: {Utility.PrettySize(SegmentSize)}";
            retStr += $"; log device: {(LogDevice == null ? "null" : LogDevice.GetType().Name)}";
            retStr += $"; mutable fraction: {MutableFraction}; fast commit mode: {(FastCommitMode ? "yes" : "no")}";
            retStr += $"; read only mode: {(ReadOnlyMode ? "yes" : "no")}";
            retStr += $"; try recover latest: {(TryRecoverLatest ? "yes" : "no")}";
            return retStr;
        }

        /// <summary>
        /// TsavoriteLog throws CommitFailureException on non-zero IDevice error codes. If TolerateDeviceFailure, TsavoriteLog
        /// will permit operations and commits to proceed as normal after the exception is thrown, even if committed
        /// data may be lost as a result of the error. Otherwise, TsavoriteLog enters a permanently errored state and
        /// prevents future operations until restarted (on a repaired IDevice).
        ///
        /// WARNING: TOLERATING DEVICE FAILURE CAN LEAD TO DATA LOSS OR CORRUPTION AND IS FOR ADVANCED USERS ONLY
        /// </summary>
        public bool TolerateDeviceFailure = false;

        internal LogSettings GetLogSettings()
        {
            return new LogSettings
            {
                LogDevice = LogDevice,
                PageSizeBits = Utility.NumBitsPreviousPowerOf2(PageSize),
                SegmentSizeBits = Utility.NumBitsPreviousPowerOf2(SegmentSize),
                MemorySizeBits = ReadOnlyMode ? 0 : Utility.NumBitsPreviousPowerOf2(MemorySize),
                ReadCopyOptions = ReadCopyOptions.None,
                MutableFraction = MutableFraction,
                ObjectLogDevice = null,
                ReadCacheSettings = null,
            };
        }
    }
}