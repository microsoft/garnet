// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server.Vector.QueryEngine
{
    /// <summary>
    /// Defines the execution strategy for a vector search query.
    /// The strategy is chosen based on filter selectivity and index availability.
    /// </summary>
    internal enum ExecutionStrategy
    {
        /// <summary>
        /// Phase 1: Run DiskANN search first, then evaluate filter on results.
        /// Best for high selectivity (>10% match) or when no index is available.
        /// </summary>
        PostFilter,

        /// <summary>
        /// Phase 2: Query attribute index first to get candidate IDs, then compute distances.
        /// Best for very low selectivity (&lt;0.1% match) where few candidates exist.
        /// </summary>
        PreFilter,

        /// <summary>
        /// Phase 2: Build a bitmap from index, pass to DiskANN for constrained graph traversal.
        /// Best for medium selectivity (0.1%-10%) where bitmap-guided search avoids wasted I/O.
        /// </summary>
        FilterAware,
    }
}
