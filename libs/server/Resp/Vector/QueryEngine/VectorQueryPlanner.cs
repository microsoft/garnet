// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;

using Garnet.server.Vector.Filter;

namespace Garnet.server.Vector.QueryEngine
{
    /// <summary>
    /// Query planner that analyzes filter expressions and selects the optimal execution strategy.
    /// 
    /// Strategy selection uses a hybrid approach:
    /// 1. Selectivity-based thresholds (fast, simple)
    /// 2. Cost estimation (when multiple strategies are viable)
    /// 
    /// Thresholds (tunable):
    ///   selectivity &lt; 0.1%   → PreFilter  (very few matches, scan candidates directly)
    ///   selectivity 0.1%-10%  → FilterAware (moderate matches, bitmap-guided DiskANN)
    ///   selectivity &gt; 10%   → PostFilter  (most vectors match, let DiskANN run freely)
    /// </summary>
    internal sealed class VectorQueryPlanner
    {
        /// <summary>
        /// Below this selectivity, use pre-filtering (query index first, then compute distances).
        /// </summary>
        private const double PreFilterThreshold = 0.001; // 0.1%

        /// <summary>
        /// Below this selectivity (but above PreFilter), use filter-aware DiskANN search.
        /// Above this, use post-filtering.
        /// </summary>
        private const double FilterAwareThreshold = 0.10; // 10%

        private readonly AttributeIndexManager _indexManager;
        private readonly SelectivityEstimator _estimator;

        public VectorQueryPlanner(AttributeIndexManager indexManager)
        {
            _indexManager = indexManager ?? throw new ArgumentNullException(nameof(indexManager));
            _estimator = new SelectivityEstimator(indexManager);
        }

        /// <summary>
        /// Build an execution plan for a vector search query with filter.
        /// </summary>
        /// <param name="filter">Parsed filter expression AST (null if no filter).</param>
        /// <param name="filterString">Original filter string.</param>
        /// <param name="requestedCount">Number of results requested (K).</param>
        /// <param name="totalVectors">Total number of vectors in the set.</param>
        /// <returns>A query plan describing how to execute the search.</returns>
        public QueryPlan BuildPlan(Expr filter, string filterString, int requestedCount, long totalVectors)
        {
            // No filter → always post-filter (just DiskANN search, no filtering needed)
            if (filter == null || string.IsNullOrEmpty(filterString))
            {
                return new QueryPlan
                {
                    Strategy = ExecutionStrategy.PostFilter,
                    Filter = null,
                    FilterString = null,
                    IndexNames = [],
                    EstimatedSelectivity = 1.0,
                    EstimatedCandidates = totalVectors,
                    EstimatedCost = EstimateCost(ExecutionStrategy.PostFilter, 1.0, totalVectors, requestedCount, false),
                };
            }

            // Estimate selectivity
            var selectivity = _estimator.Estimate(filter);
            var estimatedCandidates = (long)(selectivity * totalVectors);

            // Find available indexes for this filter
            var availableIndexes = _indexManager.GetIndexesForFilter(filter);
            var hasIndexes = availableIndexes.Count > 0;
            var indexNames = availableIndexes.Select(i => i.FieldName).ToList();

            // Choose strategy based on selectivity and index availability
            var strategy = ChooseStrategy(selectivity, hasIndexes, estimatedCandidates, requestedCount);

            // Estimate cost for chosen strategy
            var cost = EstimateCost(strategy, selectivity, totalVectors, requestedCount, hasIndexes);

            return new QueryPlan
            {
                Strategy = strategy,
                Filter = filter,
                FilterString = filterString,
                IndexNames = indexNames,
                EstimatedSelectivity = selectivity,
                EstimatedCandidates = estimatedCandidates,
                EstimatedCost = cost,
            };
        }

        /// <summary>
        /// Choose execution strategy based on selectivity thresholds and index availability.
        /// </summary>
        private static ExecutionStrategy ChooseStrategy(
            double selectivity,
            bool hasIndexes,
            long estimatedCandidates,
            int requestedCount)
        {
            // No indexes → must post-filter
            if (!hasIndexes)
                return ExecutionStrategy.PostFilter;

            // Very low selectivity: pre-filter (scan candidates directly)
            if (selectivity < PreFilterThreshold)
                return ExecutionStrategy.PreFilter;

            // Medium selectivity: bitmap-guided DiskANN search
            if (selectivity < FilterAwareThreshold)
                return ExecutionStrategy.FilterAware;

            // High selectivity: post-filter is most efficient
            return ExecutionStrategy.PostFilter;
        }

        /// <summary>
        /// Estimate execution cost for a strategy.
        /// Used for plan comparison and telemetry.
        /// </summary>
        private static ExecutionCost EstimateCost(
            ExecutionStrategy strategy,
            double selectivity,
            long totalVectors,
            int requestedCount,
            bool hasIndexes)
        {
            return strategy switch
            {
                ExecutionStrategy.PostFilter => new ExecutionCost
                {
                    IndexLookups = 0,
                    // Need to search more candidates to get enough matches after filtering
                    DistanceComputations = selectivity > 0 ? requestedCount / selectivity : requestedCount * 10,
                    IOOperations = selectivity > 0 ? requestedCount / selectivity : requestedCount * 10,
                    FilterEvaluations = selectivity > 0 ? requestedCount / selectivity : requestedCount * 10,
                },

                ExecutionStrategy.PreFilter => new ExecutionCost
                {
                    IndexLookups = 1,
                    DistanceComputations = Math.Min(selectivity * totalVectors, totalVectors),
                    IOOperations = Math.Min(selectivity * totalVectors, totalVectors),
                    FilterEvaluations = 0, // No post-filtering needed
                },

                ExecutionStrategy.FilterAware => new ExecutionCost
                {
                    IndexLookups = 1,
                    // DiskANN navigates graph but skips non-matching vectors
                    DistanceComputations = requestedCount * 1.5, // Slightly more than K due to graph navigation
                    IOOperations = requestedCount * 2, // Some wasted hops
                    FilterEvaluations = 0, // Bitmap check instead of expression eval
                },

                _ => throw new ArgumentException($"Unknown strategy: {strategy}")
            };
        }
    }
}
