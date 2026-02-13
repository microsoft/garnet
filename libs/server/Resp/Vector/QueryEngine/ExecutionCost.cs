// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server.Vector.QueryEngine
{
    /// <summary>
    /// Estimated cost of executing a query plan.
    /// Used by the query planner to compare strategies and select the cheapest one.
    /// </summary>
    internal sealed class ExecutionCost
    {
        /// <summary>
        /// Estimated number of index lookup operations.
        /// </summary>
        public double IndexLookups { get; init; }

        /// <summary>
        /// Estimated number of vector distance computations.
        /// </summary>
        public double DistanceComputations { get; init; }

        /// <summary>
        /// Estimated number of I/O operations (data loads from store).
        /// </summary>
        public double IOOperations { get; init; }

        /// <summary>
        /// Estimated number of filter evaluations (post-filter expression evals).
        /// </summary>
        public double FilterEvaluations { get; init; }

        /// <summary>
        /// Total composite cost score (lower is better).
        /// Weights can be tuned based on empirical measurements.
        /// </summary>
        public double TotalCost =>
            (IndexLookups * CostWeights.IndexLookup) +
            (DistanceComputations * CostWeights.DistanceComputation) +
            (IOOperations * CostWeights.IOOperation) +
            (FilterEvaluations * CostWeights.FilterEvaluation);

        public override string ToString()
            => $"Cost[total={TotalCost:F1}, idx={IndexLookups:F0}, dist={DistanceComputations:F0}, io={IOOperations:F0}, filter={FilterEvaluations:F0}]";

        /// <summary>
        /// Cost weights for different operations. Tunable based on benchmarks.
        /// Relative values: I/O is most expensive, index lookup is cheapest.
        /// </summary>
        internal static class CostWeights
        {
            public const double IndexLookup = 0.1;
            public const double DistanceComputation = 1.0;
            public const double IOOperation = 10.0;
            public const double FilterEvaluation = 0.5;
        }
    }
}
