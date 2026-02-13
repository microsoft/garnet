// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Threading;

namespace Garnet.server.Vector.QueryEngine
{
    /// <summary>
    /// Hash-based attribute index for fast equality lookups.
    /// Maps attribute values to sets of vector IDs.
    /// Thread-safe via ReaderWriterLockSlim.
    /// </summary>
    internal sealed class HashIndex : IAttributeIndex
    {
        private readonly Dictionary<string, HashSet<long>> _index = new(StringComparer.OrdinalIgnoreCase);
        private readonly ReaderWriterLockSlim _lock = new();
        private readonly IndexStatistics _statistics = new();

        public string FieldName { get; }
        public AttributeIndexType IndexType => AttributeIndexType.Hash;
        public IndexStatistics Statistics => _statistics;

        public HashIndex(string fieldName)
        {
            FieldName = fieldName ?? throw new ArgumentNullException(nameof(fieldName));
        }

        public void Add(long vectorId, object value)
        {
            var key = NormalizeKey(value);
            if (key == null) return;

            _lock.EnterWriteLock();
            try
            {
                if (!_index.TryGetValue(key, out var set))
                {
                    set = new HashSet<long>();
                    _index[key] = set;
                }

                if (set.Add(vectorId))
                {
                    _statistics.TotalEntries++;
                    _statistics.DistinctValues = _index.Count;
                }
            }
            finally
            {
                _lock.ExitWriteLock();
            }
        }

        public void Remove(long vectorId, object value)
        {
            var key = NormalizeKey(value);
            if (key == null) return;

            _lock.EnterWriteLock();
            try
            {
                if (_index.TryGetValue(key, out var set))
                {
                    if (set.Remove(vectorId))
                    {
                        _statistics.TotalEntries--;

                        if (set.Count == 0)
                        {
                            _index.Remove(key);
                            _statistics.DistinctValues = _index.Count;
                        }
                    }
                }
            }
            finally
            {
                _lock.ExitWriteLock();
            }
        }

        public IReadOnlySet<long> GetEqual(object value)
        {
            var key = NormalizeKey(value);
            if (key == null) return EmptySet.Instance;

            _lock.EnterReadLock();
            try
            {
                if (_index.TryGetValue(key, out var set))
                {
                    // Return a snapshot to avoid concurrent modification issues
                    return new HashSet<long>(set);
                }
                return EmptySet.Instance;
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }

        public IReadOnlySet<long> GetRange(double min, double max, bool minInclusive = true, bool maxInclusive = false)
        {
            // Hash index does not support range queries - return empty
            return EmptySet.Instance;
        }

        public void Clear()
        {
            _lock.EnterWriteLock();
            try
            {
                _index.Clear();
                _statistics.TotalEntries = 0;
                _statistics.DistinctValues = 0;
            }
            finally
            {
                _lock.ExitWriteLock();
            }
        }

        /// <summary>
        /// Estimate selectivity for an equality predicate on this field.
        /// Assumes uniform distribution: selectivity = 1 / distinctValues.
        /// </summary>
        public double EstimateEqualitySelectivity()
        {
            if (_statistics.DistinctValues == 0 || _statistics.TotalEntries == 0)
                return 1.0; // No data - assume everything matches (fallback to post-filter)

            return 1.0 / _statistics.DistinctValues;
        }

        /// <summary>
        /// Estimate selectivity for a specific value.
        /// </summary>
        public double EstimateEqualitySelectivity(object value)
        {
            var key = NormalizeKey(value);
            if (key == null || _statistics.TotalEntries == 0) return 0.0;

            _lock.EnterReadLock();
            try
            {
                if (_index.TryGetValue(key, out var set))
                {
                    return (double)set.Count / _statistics.TotalEntries;
                }
                return 0.0;
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }

        private static string NormalizeKey(object value)
        {
            if (value == null) return null;
            if (value is string s) return s;
            if (value is double d) return d.ToString("G");
            if (value is bool b) return b ? "true" : "false";
            return value.ToString();
        }

        /// <summary>
        /// Singleton empty set to avoid allocations.
        /// </summary>
        private sealed class EmptySet : IReadOnlySet<long>
        {
            public static readonly EmptySet Instance = new();
            public int Count => 0;
            public bool Contains(long item) => false;
            public bool IsProperSubsetOf(IEnumerable<long> other) => true;
            public bool IsProperSupersetOf(IEnumerable<long> other) => false;
            public bool IsSubsetOf(IEnumerable<long> other) => true;
            public bool IsSupersetOf(IEnumerable<long> other) => false;
            public bool Overlaps(IEnumerable<long> other) => false;
            public bool SetEquals(IEnumerable<long> other) => false;
            public IEnumerator<long> GetEnumerator() { yield break; }
            System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
        }
    }
}
