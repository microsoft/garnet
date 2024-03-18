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
    /// Scan iterator interface for Tsavorite log
    /// </summary>
    /// <typeparam name="Key"></typeparam>
    /// <typeparam name="Value"></typeparam>
    public interface ITsavoriteScanIterator<Key, Value> : IDisposable
    {
        /// <summary>
        /// Gets reference to current key
        /// </summary>
        /// <returns></returns>
        ref Key GetKey();

        /// <summary>
        /// Gets reference to current value
        /// </summary>
        /// <returns></returns>
        ref Value GetValue();

        /// <summary>
        /// Get next record
        /// </summary>
        /// <param name="recordInfo"></param>
        /// <returns>True if record found, false if end of scan</returns>
        bool GetNext(out RecordInfo recordInfo);

        /// <summary>
        /// Get next record
        /// </summary>
        /// <param name="recordInfo"></param>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <returns>True if record found, false if end of scan</returns>
        bool GetNext(out RecordInfo recordInfo, out Key key, out Value value);

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