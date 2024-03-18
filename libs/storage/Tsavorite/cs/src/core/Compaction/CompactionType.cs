// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// Type of log compaction
    /// </summary>
    public enum CompactionType
    {
        /// <summary>
        /// Scan from untilAddress to read-only address to check for record liveness checking
        /// </summary>
        Scan,

        /// <summary>
        /// Lookup each record in compaction range, for record liveness checking using hash chain
        /// </summary>
        Lookup,
    }
}