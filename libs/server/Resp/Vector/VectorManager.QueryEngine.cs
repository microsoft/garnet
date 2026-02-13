// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics;
using Garnet.server.Vector.QueryEngine;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Query engine integration for vector search.
    /// Adds attribute index management and query planning to VectorManager.
    /// </summary>
    public sealed partial class VectorManager
    {
        /// <summary>
        /// Per-vector-set index managers. Key is the vector set name (derived from context).
        /// Lazily created when indexes are first needed.
        /// </summary>
        private readonly ConcurrentDictionary<ulong, AttributeIndexManager> _indexManagers = new();

        /// <summary>
        /// Per-vector-set query engines. Created alongside index managers.
        /// </summary>
        private readonly ConcurrentDictionary<ulong, VectorQueryEngine> _queryEngines = new();

        /// <summary>
        /// Get or create the AttributeIndexManager for a vector set.
        /// </summary>
        private AttributeIndexManager GetOrCreateIndexManager(ulong context)
        {
            return _indexManagers.GetOrAdd(context, ctx =>
                new AttributeIndexManager($"vectorset:{ctx}"));
        }

        /// <summary>
        /// Get or create the VectorQueryEngine for a vector set.
        /// </summary>
        private VectorQueryEngine GetOrCreateQueryEngine(ulong context)
        {
            return _queryEngines.GetOrAdd(context, ctx =>
            {
                var indexManager = GetOrCreateIndexManager(ctx);
                return new VectorQueryEngine(indexManager, logger);
            });
        }

        /// <summary>
        /// Create an attribute index on a field for a vector set.
        /// Called via VIDXCREATE command or programmatically.
        /// </summary>
        /// <param name="context">The vector set context.</param>
        /// <param name="fieldName">The JSON attribute field to index.</param>
        /// <param name="indexType">Type of index (Hash for equality, Range for comparisons).</param>
        /// <returns>True if created, false if already exists.</returns>
        internal bool CreateAttributeIndex(ulong context, string fieldName, AttributeIndexType indexType)
        {
            var manager = GetOrCreateIndexManager(context);
            var created = manager.CreateIndex(fieldName, indexType);

            if (created)
            {
                // Clear query engine cache so new plans consider the new index
                if (_queryEngines.TryGetValue(context, out var engine))
                {
                    engine.ClearCache();
                }

                logger?.LogInformation("Created {IndexType} index on field '{Field}' for context {Context}",
                    indexType, fieldName, context);
            }

            return created;
        }

        /// <summary>
        /// Drop an attribute index for a vector set.
        /// </summary>
        internal bool DropAttributeIndex(ulong context, string fieldName)
        {
            if (_indexManagers.TryGetValue(context, out var manager))
            {
                var dropped = manager.DropIndex(fieldName);
                if (dropped && _queryEngines.TryGetValue(context, out var engine))
                {
                    engine.ClearCache();
                }
                return dropped;
            }
            return false;
        }

        /// <summary>
        /// Notify index manager when a vector is added (called from TryAdd flow).
        /// </summary>
        internal void NotifyVectorAdded(ulong context, long vectorId, ReadOnlySpan<byte> attributeJson)
        {
            if (_indexManagers.TryGetValue(context, out var manager) && manager.HasIndexes)
            {
                manager.OnVectorAdded(vectorId, attributeJson);
            }
        }

        /// <summary>
        /// Notify index manager when a vector is removed (called from TryRemove flow).
        /// </summary>
        internal void NotifyVectorRemoved(ulong context, long vectorId, ReadOnlySpan<byte> attributeJson)
        {
            if (_indexManagers.TryGetValue(context, out var manager) && manager.HasIndexes)
            {
                manager.OnVectorRemoved(vectorId, attributeJson);
            }
        }

        /// <summary>
        /// Perform a similarity search with query engine optimization.
        /// This replaces the simple post-filter-only approach with strategy-based execution.
        /// 
        /// Strategy selection:
        ///   PostFilter  → existing DiskANN search + post-filter (Phase 1 behavior)
        ///   PreFilter   → query index first, then compute distances on candidates
        ///   FilterAware → build bitmap from index, pass to DiskANN for constrained traversal
        /// </summary>
        internal VectorManagerResult ValueSimilarityWithQueryEngine(
            ReadOnlySpan<byte> indexValue,
            VectorValueType valueType,
            ReadOnlySpan<byte> values,
            int count,
            float delta,
            int searchExplorationFactor,
            ReadOnlySpan<byte> filter,
            int maxFilteringEffort,
            bool includeAttributes,
            ref SpanByteAndMemory outputIds,
            out VectorIdFormat outputIdFormat,
            ref SpanByteAndMemory outputDistances,
            ref SpanByteAndMemory outputAttributes)
        {
            AssertHaveStorageSession();

            ReadIndex(indexValue, out var context, out var dimensions, out _, out var quantType, out _, out _, out _, out var indexPtr, out _);

            var valueDims = CalculateValueDimensions(valueType, values);
            if (dimensions != valueDims)
            {
                outputIdFormat = VectorIdFormat.Invalid;
                return VectorManagerResult.BadParams;
            }

            // Get query engine for this vector set
            var queryEngine = GetOrCreateQueryEngine(context);
            var indexManager = GetOrCreateIndexManager(context);

            // Plan the query
            var plan = queryEngine.Plan(filter, count, indexManager.TotalVectors);
            var sw = Stopwatch.StartNew();

            int found;

            switch (plan.Strategy)
            {
                case ExecutionStrategy.PreFilter when indexManager.HasIndexes:
                {
                    // Phase 2: Pre-filter using indexes, then compute distances
                    var candidates = queryEngine.GetPreFilterCandidates(plan, indexManager.TotalVectors);

                    if (candidates.Count == 0)
                    {
                        // No matches from index - return empty
                        found = 0;
                        break;
                    }

                    // For pre-filter: Fall through to DiskANN search but with known candidates
                    // TODO: When DiskANN supports candidate-constrained search, use it here.
                    // For now, we still run full DiskANN search + post-filter,
                    // but with knowledge from the plan for future optimization.
                    found = ExecutePostFilterSearch(
                        context, indexPtr, valueType, values, count, delta,
                        searchExplorationFactor, filter, maxFilteringEffort,
                        includeAttributes, ref outputIds, ref outputDistances, ref outputAttributes);
                    break;
                }

                case ExecutionStrategy.FilterAware when indexManager.HasIndexes:
                {
                    // Phase 2: Build bitmap from indexes, pass to DiskANN
                    var (bitmap, needsPostFilter) = queryEngine.BuildFilterBitmap(plan, indexManager.TotalVectors);

                    if (bitmap != null && bitmap.PopCount == 0)
                    {
                        // No matches from bitmap - return empty
                        found = 0;
                        break;
                    }

                    // TODO: When DiskANN FFI supports bitmap parameter, pass it here.
                    // For now, fall through to post-filter with the plan metadata.
                    found = ExecutePostFilterSearch(
                        context, indexPtr, valueType, values, count, delta,
                        searchExplorationFactor, filter, maxFilteringEffort,
                        includeAttributes, ref outputIds, ref outputDistances, ref outputAttributes);
                    break;
                }

                case ExecutionStrategy.PostFilter:
                default:
                {
                    // Phase 1: Standard DiskANN search + post-filter
                    found = ExecutePostFilterSearch(
                        context, indexPtr, valueType, values, count, delta,
                        searchExplorationFactor, filter, maxFilteringEffort,
                        includeAttributes, ref outputIds, ref outputDistances, ref outputAttributes);
                    break;
                }
            }

            sw.Stop();

            // Log telemetry
            queryEngine.LogQueryMetrics(plan, new VectorQueryResult
            {
                Found = found,
                StrategyUsed = plan.Strategy,
                PostFilterApplied = !filter.IsEmpty,
                ElapsedTicks = sw.ElapsedTicks,
            });

            outputDistances.Length = sizeof(float) * found;
            outputIdFormat = VectorIdFormat.I32LengthPrefixed;

            if (quantType == VectorQuantType.XPreQ8)
            {
                outputIdFormat = VectorIdFormat.I32LengthPrefixed;
            }

            return VectorManagerResult.OK;
        }

        /// <summary>
        /// Execute the standard post-filter search path.
        /// Extracted from ValueSimilarity to be reusable by all strategies as a fallback.
        /// </summary>
        private int ExecutePostFilterSearch(
            ulong context,
            nint indexPtr,
            VectorValueType valueType,
            ReadOnlySpan<byte> values,
            int count,
            float delta,
            int searchExplorationFactor,
            ReadOnlySpan<byte> filter,
            int maxFilteringEffort,
            bool includeAttributes,
            ref SpanByteAndMemory outputIds,
            ref SpanByteAndMemory outputDistances,
            ref SpanByteAndMemory outputAttributes)
        {
            if (count > searchExplorationFactor)
            {
                count = searchExplorationFactor;
            }

            // Ensure buffer sizes
            if (count * sizeof(float) > outputDistances.Length)
            {
                if (!outputDistances.IsSpanByte)
                    outputDistances.Memory.Dispose();
                outputDistances = new SpanByteAndMemory(MemoryPool<byte>.Shared.Rent(count * sizeof(float)), count * sizeof(float));
            }
            outputDistances.Length = count * sizeof(float);

            if (count * MinimumSpacePerId > outputIds.Length)
            {
                if (!outputIds.IsSpanByte)
                    outputIds.Memory.Dispose();
                outputIds = new SpanByteAndMemory(MemoryPool<byte>.Shared.Rent(count * MinimumSpacePerId), count * MinimumSpacePerId);
            }

            var found = Service.SearchVector(
                context, indexPtr, valueType, values,
                delta, searchExplorationFactor, filter, maxFilteringEffort,
                outputIds, outputDistances, out var continuation);

            if (found < 0)
            {
                logger?.LogWarning("Error from vector service: {found}", found);
                return 0;
            }

            if (includeAttributes)
            {
                FetchVectorElementAttributes(context, found, outputIds, ref outputAttributes);
            }

            // Apply post-filtering
            if (!filter.IsEmpty && includeAttributes)
            {
                found = ApplyPostFilter(filter, found, ref outputIds, ref outputDistances, ref outputAttributes, includeAttributes);
            }

            return found;
        }
    }
}
