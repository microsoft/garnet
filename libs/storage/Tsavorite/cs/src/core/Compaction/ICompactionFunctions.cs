// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// Optional functions to be called during compaction.
    /// </summary>
    public interface ICompactionFunctions
    {
        /// <summary>
        /// Checks if record in the Tsavorite log is logically deleted.
        /// If the record was deleted the usual Delete() (i.e. its tombstone is set), then this function is not called for it.
        /// </summary>
        /// <remarks>
        /// One possible scenario is if Tsavorite is used to store reference counted records. If the refcount reaches zero
        /// it can be considered to be no longer relevant and compaction can skip the record.
        /// </remarks>
        bool IsDeleted<TSourceLogRecord>(in TSourceLogRecord logRecord)
            where TSourceLogRecord : ISourceLogRecord;
    }

    internal struct DefaultCompactionFunctions : ICompactionFunctions
    {
        public bool IsDeleted<TSourceLogRecord>(in TSourceLogRecord logRecord)
            where TSourceLogRecord : ISourceLogRecord
            => false;
    }
}