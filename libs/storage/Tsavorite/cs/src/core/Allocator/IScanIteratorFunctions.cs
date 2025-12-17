// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Tsavorite.core
{
    using static LogAddress;

    /// <summary>
    /// Result for records sent to the callback during ScanCursor.
    /// </summary>
    [Flags]
    public enum CursorRecordResult
    {
        /// <summary>
        /// The record meets the callback's filter criteria for this scan (or there is no filter); increment the cursor counter.
        /// </summary>
        Accept = 1,

        /// <summary>
        /// The record does not meet the callback's filter criteria for this scan; do not increment the cursor counter
        /// </summary>
        Skip = 2,

        /// <summary>
        /// End the current cursor batch (as if "count" had been met); return a valid cursor for the next ScanCursor call
        /// </summary>
        EndBatch = 4,

        /// <summary>
        /// Retry the last record when returning a valid cursor
        /// </summary>
        RetryLastRecord = 8,
    }

    /// <summary>
    /// Callback functions for log scan or key-version iteration
    /// </summary>
    public interface IScanIteratorFunctions
    {
        /// <summary>Iteration is starting.</summary>
        /// <param name="beginAddress">Start address of the scan</param>
        /// <param name="endAddress">End address of the scan; if iterating key versions, this is <see cref="kInvalidAddress"/></param>
        /// <returns>True to continue iteration, else false</returns>
        bool OnStart(long beginAddress, long endAddress);

        /// <summary>Read the next record in the iteration.</summary>
        /// <param name="logRecord">Reference to the current log record's info</param>
        /// <param name="recordMetadata">Record metadata, including <see cref="RecordInfo"/> and the current record's logical address</param>
        /// <param name="numberOfRecords">The number of records accepted so far, not including the current one.</param>
        /// <param name="cursorRecordResult">Indicates whether the current record was accepted, or whether to end the current ScanCursor call.
        ///     Ignored for non-cursor Scans; set to <see cref="CursorRecordResult.Accept"/>.</param>
        /// <returns>True to continue iteration, else false</returns>
        bool Reader<TSourceLogRecord>(in TSourceLogRecord logRecord, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
            where TSourceLogRecord : ISourceLogRecord;

        /// <summary>Iteration is complete.</summary>
        /// <param name="completed">If true, the iteration completed; else scanFunctions.*Reader() returned false to stop the iteration.</param>
        /// <param name="numberOfRecords">The number of records returned before the iteration stopped.</param>
        void OnStop(bool completed, long numberOfRecords);

        /// <summary>An exception was thrown on iteration (likely during <see name="Reader"/>.</summary>
        /// <param name="exception">The exception that was thrown.</param>
        /// <param name="numberOfRecords">The number of records returned, including the current one, before the exception.</param>
        void OnException(Exception exception, long numberOfRecords);
    }

    internal interface IPushScanIterator
    {
        bool BeginGetPrevInMemory(ReadOnlySpan<byte> key, out LogRecord logRecord, out bool continueOnDisk);
        void EndGetPrevInMemory();

        /// <summary>
        /// When beginning a cursor scan, if it is not the last cursor returned, snap it to the preceding logical address boundary.
        /// </summary>
        bool SnapCursorToLogicalAddress(ref long cursor);
    }
}