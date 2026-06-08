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
        /// Lookup each record in compaction range, for record liveness checking using hash chain - no data loss
        /// (to delete actual data files from disk, take a checkpoint after compaction).
        /// Recommended for production use.
        /// </summary>
        Lookup,

        /// <summary>
        /// Scan from untilAddress to read-only address to check for record liveness checking - no data loss
        /// (to delete actual data files from disk, take a checkpoint after compaction).
        /// NOT RECOMMENDED: this strategy builds a temporary parallel KV index proportional to the keyspace,
        /// causing significant transient memory use. Prefer Lookup.
        /// </summary>
        Scan,
    }
}