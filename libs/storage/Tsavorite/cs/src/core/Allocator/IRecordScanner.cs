// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Tsavorite.core
{
    /// <summary>
    /// Scan buffering mode
    /// </summary>
    public enum ScanBufferingMode
    {
        /// <summary>
        /// Buffer only current page being scanned
        /// </summary>
        SinglePageBuffering,

        /// <summary>
        /// Buffer current and next page in scan sequence
        /// </summary>
        DoublePageBuffering,

        /// <summary>
        /// Do not buffer - with this mode, you can only scan records already in main memory
        /// </summary>
        NoBuffering
    }

    /// <summary>
    /// Internal record scanning interface for Tsavorite hybrid log; does not do uniqueness checking
    /// </summary>
    public interface IRecordScanner<TKey, TValue> : IDisposable
    {
        /// <summary>
        /// Get next record
        /// </summary>
        /// <returns>True if record found, false if end of scan</returns>
        bool ScanNext();

        /// <summary>
        /// Get next record
        /// </summary>
        /// <param name="status">Indicates whether the conditional push went pending</param>
        /// <returns>True if record found or pending, false if end of scan</returns>
        bool IterateNext(out Status status);

        /// <summary>
        /// The starting address of the scan
        /// </summary>
        long BeginAddress { get; }

        /// <summary>
        /// The ending address of the scan
        /// </summary>
        long EndAddress { get; }

        /// <summary>
        /// The current address of the scan
        /// </summary>
        long CurrentAddress { get; }

        /// <summary>
        /// The next address for the scan
        /// </summary>
        long NextAddress { get; }

        /// <summary>
        /// Used when tracing back for key versions
        /// </summary>
        bool GetPrevInMemory<TLocalScanFunctions>(ref TKey key, TLocalScanFunctions scanFunctions, ref long numRecords, out bool continueOnDisk, out bool stop)
            where TLocalScanFunctions : IScanIteratorFunctions<TKey, TValue>;

        /// <summary>
        /// When beginning a cursor scan, if it is not the last cursor returned, snap it to the preceding logical address boundary.
        /// </summary>
        bool SnapCursorToLogicalAddress(ref long cursor);
    }
}