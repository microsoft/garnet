// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// Type of log compaction
    /// </summary>
    public enum IterationType
    {
        /// <summary>
        /// Lookup records for liveness checking using hash chain
        /// </summary>
        Lookup,

        /// <summary>
        /// Scan records for liveness checking
        /// </summary>
        Scan,
    }
}