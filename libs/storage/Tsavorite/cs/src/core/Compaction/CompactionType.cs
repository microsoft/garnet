// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// Type of lookup to use for record uniqueness checks in log compaction and kv iteration
    /// </summary>
    public enum CompactionType
    {
        /// <summary>
        /// Use temporary instance of TsavoriteKV for record liveness checking
        /// </summary>
        Scan,

        /// <summary>
        /// Lookup individual records directly in the main TsavoriteKV for liveness checking
        /// </summary>
        Lookup,
    }
}