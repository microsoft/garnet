// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;

namespace Garnet.server.Vector.QueryEngine
{
    /// <summary>
    /// Statistics about an attribute index, used for selectivity estimation and cost modeling.
    /// </summary>
    internal sealed class IndexStatistics
    {
        /// <summary>Total number of entries indexed.</summary>
        public long TotalEntries { get; set; }

        /// <summary>Number of distinct values (cardinality) in the index.</summary>
        public long DistinctValues { get; set; }

        /// <summary>Minimum value in a range index (null for hash indexes).</summary>
        public double? MinValue { get; set; }

        /// <summary>Maximum value in a range index (null for hash indexes).</summary>
        public double? MaxValue { get; set; }
    }

    /// <summary>
    /// Defines the contract for an attribute index on a vector set field.
    /// Indexes provide fast lookup of vector IDs matching a predicate.
    /// </summary>
    internal interface IAttributeIndex
    {
        /// <summary>
        /// The field name this index covers (e.g., "category", "price").
        /// </summary>
        string FieldName { get; }

        /// <summary>
        /// The type of index (Hash, Range, etc.).
        /// </summary>
        AttributeIndexType IndexType { get; }

        /// <summary>
        /// Current statistics for this index.
        /// </summary>
        IndexStatistics Statistics { get; }

        /// <summary>
        /// Add an entry to the index.
        /// </summary>
        /// <param name="vectorId">The internal vector ID.</param>
        /// <param name="value">The attribute value to index.</param>
        void Add(long vectorId, object value);

        /// <summary>
        /// Remove an entry from the index.
        /// </summary>
        /// <param name="vectorId">The internal vector ID to remove.</param>
        /// <param name="value">The attribute value (needed for hash index removal).</param>
        void Remove(long vectorId, object value);

        /// <summary>
        /// Get all vector IDs matching an equality predicate.
        /// </summary>
        /// <param name="value">The value to match.</param>
        /// <returns>Set of matching vector IDs.</returns>
        IReadOnlySet<long> GetEqual(object value);

        /// <summary>
        /// Get all vector IDs matching a range predicate.
        /// </summary>
        /// <param name="min">Minimum value (inclusive if minInclusive is true).</param>
        /// <param name="max">Maximum value (inclusive if maxInclusive is true).</param>
        /// <param name="minInclusive">Whether min boundary is inclusive.</param>
        /// <param name="maxInclusive">Whether max boundary is inclusive.</param>
        /// <returns>Set of matching vector IDs.</returns>
        IReadOnlySet<long> GetRange(double min, double max, bool minInclusive = true, bool maxInclusive = false);

        /// <summary>
        /// Clear all entries from the index.
        /// </summary>
        void Clear();
    }

    /// <summary>
    /// Types of attribute indexes supported.
    /// </summary>
    internal enum AttributeIndexType
    {
        /// <summary>Hash-based index for equality lookups. O(1) lookup.</summary>
        Hash,

        /// <summary>Sorted index for range queries. O(log n) lookup.</summary>
        Range,
    }
}
