// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server
{
    /// <summary>
    /// Type of log compaction
    /// </summary>
    public enum LogCompactionType
    {
        /// <summary>
        /// No compaction (default)
        /// </summary>
        None,

        /// <summary>
        /// Shift the begin address without compacting active records (data loss)
        /// Take a checkpoint to actually delete files from disk.
        /// </summary>
        Shift,

        /// <summary>
        /// Shift the begin address without compacting active records (data loss)
        /// Immediately deletes files - do not use if you plan to recover after failure.
        /// </summary>
        ShiftForced,

        /// <summary>
        /// Scan from untilAddress to read-only address to check for record liveness checking - no data loss
        /// (to delete actual data files from disk, take a checkpoint after compaction)
        /// </summary>
        Scan,

        /// <summary>
        /// Lookup each record in compaction range, for record liveness checking using hash chain - no data loss
        /// (to delete actual data files from disk, take a checkpoint after compaction)
        /// </summary>
        Lookup,
    }
}