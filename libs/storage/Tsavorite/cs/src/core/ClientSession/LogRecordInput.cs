// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// Wraps a source log record as an Input for copy operations (compaction, iteration).
    /// Generic over TSourceLogRecord to avoid boxing — all ISourceLogRecord implementations
    /// are value types (LogRecord, DiskLogRecord) except ITsavoriteScanIterator which is
    /// implemented by classes.
    /// </summary>
    public struct LogRecordInput<TSourceLogRecord> where TSourceLogRecord : ISourceLogRecord
    {
        public TSourceLogRecord SourceRecord;
        public bool writeToAof;
    }
}
