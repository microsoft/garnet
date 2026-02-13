// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server.Vector.QueryEngine
{
    /// <summary>
    /// Result of a vector query execution.
    /// Wraps the output buffers and metadata from the search.
    /// </summary>
    internal sealed class VectorQueryResult
    {
        /// <summary>Number of results found.</summary>
        public int Found { get; set; }

        /// <summary>The execution strategy that was used.</summary>
        public ExecutionStrategy StrategyUsed { get; set; }

        /// <summary>Whether post-filtering was applied (even in pre/filter-aware modes, as a safety net).</summary>
        public bool PostFilterApplied { get; set; }

        /// <summary>Number of candidates examined before filtering.</summary>
        public int CandidatesExamined { get; set; }

        /// <summary>Elapsed time in ticks for the search execution.</summary>
        public long ElapsedTicks { get; set; }
    }

    /// <summary>
    /// Context passed to query executors, providing access to the vector manager
    /// and DiskANN service for search operations.
    /// </summary>
    internal sealed class QueryExecutionContext
    {
        /// <summary>DiskANN context handle for the vector set.</summary>
        public ulong Context { get; init; }

        /// <summary>DiskANN index pointer.</summary>
        public nint IndexPtr { get; init; }

        /// <summary>Number of dimensions in the vectors.</summary>
        public uint Dimensions { get; init; }

        /// <summary>Vector quantization type.</summary>
        public VectorQuantType QuantType { get; init; }

        /// <summary>Total number of vectors in the set (for bitmap sizing).</summary>
        public long TotalVectors { get; init; }

        /// <summary>Reference to the AttributeIndexManager for this vector set.</summary>
        public AttributeIndexManager IndexManager { get; init; }
    }
}
