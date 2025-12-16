// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;

namespace Tsavorite.core
{
    /// <summary>
    /// Settings for record Revivification
    /// </summary>
    public class RevivificationSettings
    {
        /// <summary>
        /// Default revivification to use the full mutable region.
        /// </summary>
        public const double DefaultRevivifiableFraction = 1;

        /// <summary>
        /// Indicates whether deleted record space should be reused.
        /// <list type="bullet">
        /// <item>If this is true, then tombstoned records in the hashtable chain are revivified if possible, and a FreeList is maintained if 
        ///     <see cref="FreeRecordBins"/> is non-null and non-empty.
        /// </item>
        /// <item>If this is false, then tombstoned records in the hashtable chain will not be revivified, and no FreeList is used (regardless 
        ///     of the setting of <see cref="FreeRecordBins"/>).
        /// </item>
        /// </list>
        /// </summary>
        public bool EnableRevivification = true;

        /// <summary>
        /// What percentage of <see cref="LogSettings.MutableFraction"/>, from the tail down, is eligible for revivification, calculated as
        /// (TailAddress - ReadOnlyAddress) * RevivifiableFraction. This prevents revivifying records too close to ReadOnly for the app's usage pattern--it
        /// may be important for recent records to remain near the tail. Default is to use the full mutable region. See also <see cref="UseFreeRecordPoolForCopyToTail"/>
        /// to control whether CopyToTail operations use the FreeRecordList.
        /// </summary>
        public double RevivifiableFraction = DefaultRevivifiableFraction;

        /// <summary>
        /// Bin definitions for the free list (in addition to any in the hash chains). These must be ordered by <see cref="RevivificationBin.RecordSize"/>.
        /// </summary>
        public RevivificationBin[] FreeRecordBins;

        /// <summary>
        /// By default, when looking for FreeRecords we search only the bin for the specified size. This allows searching the next-highest bin as well.
        /// </summary>
        public int NumberOfBinsToSearch = 0;

        /// <summary>
        /// Deleted records that are to be added to a RevivificationBin are elided from the hash chain. If the bin is full, this option controls whether the
        /// record is restored (if possible) to the hash chain. This preserves them as in-chain revivifiable records, at the potential cost of having the record
        /// evicted to disk while part of the hash chain, and thus having to do an I/O only to find that the record is deleted and thus potentially unnecessary.
        /// For applications that add and delete the same keys repeatedly, this option should be set true if the FreeList is used.
        /// </summary>
        public bool RestoreDeletedRecordsIfBinIsFull = true;

        /// <summary>
        /// When doing explicit CopyToTail operations such as Compaction, CopyToTail when reading from the immutable in-memory region, or disk IO, this controls
        /// whether the allocation for the retrieved records may be satisfied from the FreeRecordPool. These operations may require that the records be allocated
        /// at the tail of the log, to remain in memory as long as possible. See also <see cref="RevivifiableFraction"/> to control how much of the mutable memory
        /// space below the log tail is eligible for revivification.
        /// </summary>
        public bool UseFreeRecordPoolForCopyToTail = true;

        /// <summary>
        /// Use power-of-2 bins with a single oversize bin.
        /// </summary>
        public static PowerOf2BinsRevivificationSettings PowerOf2Bins { get; } = new();

        /// <summary>
        /// Enable only in-tag-chain revivification; do not use FreeList
        /// </summary>
        public static RevivificationSettings InChainOnly { get; } = new();

        /// <summary>
        /// Turn off all revivification.
        /// </summary>
        public static RevivificationSettings None { get; } = new() { EnableRevivification = false };

        internal void Verify(double mutableFraction)
        {
            if (!EnableRevivification || FreeRecordBins?.Length == 0)
                return;
            if (RevivifiableFraction != DefaultRevivifiableFraction)
            {
                if (RevivifiableFraction <= 0)
                    throw new TsavoriteException($"RevivifiableFraction cannot be <= zero (unless it is {DefaultRevivifiableFraction})");
                if (RevivifiableFraction > mutableFraction)
                    throw new TsavoriteException($"RevivifiableFraction ({RevivifiableFraction}) must be <= to LogSettings.MutableFraction ({mutableFraction})");
            }
            if (FreeRecordBins is not null)
            {
                foreach (var bin in FreeRecordBins)
                    bin.Verify();
            }
        }

        /// <summary>
        /// Return a copy of these RevivificationSettings.
        /// </summary>
        public RevivificationSettings Clone()
        {
            var settings = new RevivificationSettings()
            {
                EnableRevivification = EnableRevivification,
                RevivifiableFraction = RevivifiableFraction,
                NumberOfBinsToSearch = NumberOfBinsToSearch,
                RestoreDeletedRecordsIfBinIsFull = RestoreDeletedRecordsIfBinIsFull,
                UseFreeRecordPoolForCopyToTail = UseFreeRecordPoolForCopyToTail
            };

            if (FreeRecordBins is not null)
            {
                settings.FreeRecordBins = new RevivificationBin[FreeRecordBins.Length];
                Array.Copy(FreeRecordBins, settings.FreeRecordBins, FreeRecordBins.Length);
            }
            return settings;
        }

        /// <inheritdoc/>
        public override string ToString()
            => $"enabled {EnableRevivification}, mutable% {RevivifiableFraction}, #bins {FreeRecordBins?.Length}, searchNextBin {NumberOfBinsToSearch}";
    }

    /// <summary>
    /// Settings for a Revivification bin
    /// </summary>
    public struct RevivificationBin
    {
        /// <summary>
        /// The minimum size of a record; RecordInfo + int key/value, or any key/value combination below 8 bytes total, as record size is
        /// a multiple of 8.
        /// </summary>
        public const int MinRecordSize = 16;

        /// <summary>
        /// The minimum number of records per bin.
        /// </summary>
        public const int MinRecordsPerBin = FreeRecordBin.MinRecordsPerBin;

        /// <summary>
        /// The maximum size of a record; must fit on a single page.
        /// </summary>
        public const int MaxRecordSize = 1 << LogSettings.kMaxPageSizeBits;

        /// <summary>
        /// The maximum size of a record whose size can be stored "inline" in the FreeRecord metadata. This is informational, not a limit;
        /// sizes larger than this are considered "oversize" and require calls to the allocator to determine exact record size, which is slower.
        /// </summary>
        public const int MaxInlineRecordSize = 1 << FreeRecord.kSizeBits;

        /// <summary>
        /// Scan all records in the bin for best fit.
        /// </summary>
        public const int BestFitScanAll = int.MaxValue;

        /// <summary>
        /// Use first-fit instead of best-fit.
        /// </summary>
        public const int UseFirstFit = 0;

        /// <summary>
        /// The default number of records per bin.
        /// </summary>
        public const int DefaultRecordsPerBin = 256;

        /// <summary>
        /// The maximum size of records in this partition. This should be partitioned for your app.
        /// </summary>
        public int RecordSize;

        /// <summary>
        /// The number of records for each partition. This count will be adjusted upward so the partition is cache-line aligned.
        /// </summary>
        public int NumberOfRecords = DefaultRecordsPerBin;

        /// <summary>
        /// The maximum number of entries to scan for best fit after finding first fit.
        /// </summary>
        public int BestFitScanLimit = UseFirstFit;

        /// <summary>
        /// Constructor
        /// </summary>
        public RevivificationBin()
        {
        }

        internal void Verify()
        {
            if (NumberOfRecords < MinRecordsPerBin)
                throw new TsavoriteException($"Invalid NumberOfRecords {NumberOfRecords}; must be > {MinRecordsPerBin}");
            if (BestFitScanLimit < 0)
                throw new Exception("BestFitScanLimit must be >= 0.");
        }

        /// <inheritdoc/>
        public override string ToString()
        {
            string scanStr = BestFitScanLimit switch
            {
                BestFitScanAll => "ScanAll",
                UseFirstFit => "FirstFit",
                _ => BestFitScanLimit.ToString()
            };
            return $"recSize {RecordSize}, numRecs {NumberOfRecords}, scanLimit {scanStr}";
        }
    }

    /// <summary>
    /// Default revivification bin definition: Use power-of-2 bins with a single oversize bin.
    /// </summary>
    public class PowerOf2BinsRevivificationSettings : RevivificationSettings
    {
        /// <summary>
        /// Constructor
        /// </summary>
        public PowerOf2BinsRevivificationSettings() : base()
        {
            List<RevivificationBin> binList = new();

            // Start with the 16-byte bin
            for (var size = RevivificationBin.MinRecordSize; size <= RevivificationBin.MaxInlineRecordSize; size *= 2)
                binList.Add(new RevivificationBin()
                {
                    RecordSize = size,
                    NumberOfRecords = RevivificationBin.DefaultRecordsPerBin
                });

            // Use one oversize bin.
            binList.Add(new RevivificationBin()
            {
                RecordSize = RevivificationBin.MaxRecordSize,
                NumberOfRecords = RevivificationBin.DefaultRecordsPerBin
            });
            FreeRecordBins = [.. binList];
        }
    }
}