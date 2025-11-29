// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Tsavorite.core
{
    /// <summary>
    /// Scan buffering mode when reading from disk
    /// </summary>
    public enum DiskScanBufferingMode
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
    /// Scan buffering mode for in-memory records, e.g. for copying and holding a record for Pull iterators
    /// </summary>
    public enum InMemoryScanBufferingMode
    {
        /// <summary>
        /// Buffer the current record being scanned. Automatic for Pull iteration.
        /// </summary>
        CurrentRecordBuffering,

        /// <summary>
        /// Do not buffer - with this mode, Push iteration will hold the epoch during each record's push to the client
        /// </summary>
        NoBuffering
    }

    /// <summary>
    /// Scan iterator interface for Tsavorite log
    /// </summary>
    public interface ITsavoriteScanIterator : ISourceLogRecord, IDisposable
    {
        /// <summary>
        /// Get next record
        /// </summary>
        /// <returns>True if record found, false if end of scan</returns>
        bool GetNext();

        /// <summary>
        /// Current address
        /// </summary>
        long CurrentAddress { get; }

        /// <summary>
        /// Next address
        /// </summary>
        long NextAddress { get; }

        /// <summary>
        /// The starting address of the scan
        /// </summary>
        long BeginAddress { get; }

        /// <summary>
        /// The ending address of the scan
        /// </summary>
        long EndAddress { get; }
    }
}