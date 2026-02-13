// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Text;
using Garnet.server.Vector.Filter;
using Microsoft.Extensions.Logging;

namespace Garnet.server.Vector.QueryEngine
{
    /// <summary>
    /// Main orchestrator for vector search queries with filtering.
    /// 
    /// Coordinates:
    /// 1. Filter parsing (reuses existing tokenizer/parser)
    /// 2. Query planning (selectivity estimation + strategy selection)
    /// 3. Execution dispatch (delegates to the appropriate strategy)
    /// 4. Telemetry logging
    /// 
    /// Design: Minimal query engine inspired by DuckDB's simplicity.
    /// Strategy selection uses switch dispatch (not registry/plugin) to keep overhead minimal.
    /// </summary>
    internal sealed class VectorQueryEngine
    {
        private readonly VectorQueryPlanner _planner;
        private readonly AttributeIndexManager _indexManager;
        private readonly ILogger _logger;

        // Cached parsed expressions to avoid re-parsing the same filter string
        // Key: filter string, Value: parsed AST
        private readonly Dictionary<string, Expr> _expressionCache = new();

        public VectorQueryEngine(AttributeIndexManager indexManager, ILogger logger = null)
        {
            _indexManager = indexManager ?? throw new ArgumentNullException(nameof(indexManager));
            _planner = new VectorQueryPlanner(indexManager);
            _logger = logger;
        }

        /// <summary>
        /// Plan a query: parse the filter, estimate selectivity, choose strategy.
        /// Returns a plan that can be inspected (for EXPLAIN) or executed.
        /// </summary>
        /// <param name="filterBytes">Raw filter bytes from VSIM command.</param>
        /// <param name="requestedCount">Number of results requested (K).</param>
        /// <param name="totalVectors">Total vectors in the set.</param>
        /// <returns>A query plan describing the optimal execution strategy.</returns>
        public QueryPlan Plan(ReadOnlySpan<byte> filterBytes, int requestedCount, long totalVectors)
        {
            // Parse filter
            string filterString = null;
            Expr filterExpr = null;

            if (!filterBytes.IsEmpty)
            {
                filterString = Encoding.UTF8.GetString(filterBytes);
                filterExpr = ParseFilter(filterString);
            }

            // Build plan
            return _planner.BuildPlan(filterExpr, filterString, requestedCount, totalVectors);
        }

        /// <summary>
        /// Build a filter bitmap for use in filter-aware DiskANN search.
        /// </summary>
        /// <param name="plan">The query plan (must have Strategy = FilterAware or PreFilter).</param>
        /// <param name="totalVectors">Total vectors for bitmap sizing.</param>
        /// <returns>
        /// A bitmap and a flag indicating if post-filtering is still needed
        /// (when some predicates couldn't be resolved via indexes).
        /// </returns>
        public (FilterBitmap bitmap, bool needsPostFilter) BuildFilterBitmap(QueryPlan plan, long totalVectors)
        {
            if (plan.Filter == null)
                return (null, false);

            var builder = new FilterBitmapBuilder(_indexManager, totalVectors);
            var bitmap = builder.Build(plan.Filter);

            return (bitmap, builder.HasUnresolvedPredicates);
        }

        /// <summary>
        /// Get candidate vector IDs from indexes for pre-filter strategy.
        /// </summary>
        /// <param name="plan">The query plan.</param>
        /// <param name="totalVectors">Total vectors for bitmap sizing.</param>
        /// <returns>Set of candidate vector IDs matching the filter.</returns>
        public IReadOnlySet<long> GetPreFilterCandidates(QueryPlan plan, long totalVectors)
        {
            if (plan.Filter == null)
                return new HashSet<long>();

            var builder = new FilterBitmapBuilder(_indexManager, totalVectors);
            var bitmap = builder.Build(plan.Filter);

            // Convert bitmap to ID set
            var candidates = new HashSet<long>();
            foreach (var id in bitmap.EnumerateSetBits())
            {
                candidates.Add(id);
            }

            return candidates;
        }

        /// <summary>
        /// Log query execution metrics for monitoring and debugging.
        /// </summary>
        public void LogQueryMetrics(QueryPlan plan, VectorQueryResult result)
        {
            if (_logger == null) return;

            var elapsed = result.ElapsedTicks > 0
                ? TimeSpan.FromTicks(result.ElapsedTicks).TotalMilliseconds
                : 0;

            _logger.LogDebug(
                "VectorQuery: strategy={Strategy}, selectivity={Selectivity:P2}, " +
                "candidates={Candidates}, found={Found}, " +
                "postFilter={PostFilter}, elapsed={Elapsed:F2}ms",
                plan.Strategy,
                plan.EstimatedSelectivity,
                plan.EstimatedCandidates,
                result.Found,
                result.PostFilterApplied,
                elapsed);
        }

        /// <summary>
        /// Parse a filter string into an expression AST, with caching.
        /// </summary>
        private Expr ParseFilter(string filterString)
        {
            if (string.IsNullOrWhiteSpace(filterString))
                return null;

            // Check cache
            if (_expressionCache.TryGetValue(filterString, out var cached))
                return cached;

            try
            {
                var tokens = VectorFilterTokenizer.Tokenize(filterString);
                var expr = VectorFilterParser.ParseExpression(tokens, 0, out _);

                // Cache for reuse (bounded to prevent memory leak)
                if (_expressionCache.Count < 1000)
                {
                    _expressionCache[filterString] = expr;
                }

                return expr;
            }
            catch (Exception ex)
            {
                _logger?.LogWarning(ex, "Failed to parse filter expression: {Filter}", filterString);
                return null;
            }
        }

        /// <summary>
        /// Clear the expression cache (e.g., when indexes change).
        /// </summary>
        public void ClearCache()
        {
            _expressionCache.Clear();
        }
    }
}
