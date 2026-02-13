// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Garnet.server.Vector.QueryEngine
{
    /// <summary>
    /// Sorted attribute index for range queries.
    /// Uses a SortedDictionary for O(log n) range lookups.
    /// Thread-safe via ReaderWriterLockSlim.
    /// </summary>
    internal sealed class RangeIndex : IAttributeIndex
    {
        private readonly SortedDictionary<double, HashSet<long>> _index = new();
        private readonly ReaderWriterLockSlim _lock = new();
        private readonly IndexStatistics _statistics = new();

        public string FieldName { get; }
        public AttributeIndexType IndexType => AttributeIndexType.Range;
        public IndexStatistics Statistics => _statistics;

        public RangeIndex(string fieldName)
        {
            FieldName = fieldName ?? throw new ArgumentNullException(nameof(fieldName));
        }

        public void Add(long vectorId, object value)
        {
            if (!TryConvertToDouble(value, out var numericValue)) return;

            _lock.EnterWriteLock();
            try
            {
                if (!_index.TryGetValue(numericValue, out var set))
                {
                    set = new HashSet<long>();
                    _index[numericValue] = set;
                }

                if (set.Add(vectorId))
                {
                    _statistics.TotalEntries++;
                    _statistics.DistinctValues = _index.Count;
                    UpdateMinMax();
                }
            }
            finally
            {
                _lock.ExitWriteLock();
            }
        }

        public void Remove(long vectorId, object value)
        {
            if (!TryConvertToDouble(value, out var numericValue)) return;

            _lock.EnterWriteLock();
            try
            {
                if (_index.TryGetValue(numericValue, out var set))
                {
                    if (set.Remove(vectorId))
                    {
                        _statistics.TotalEntries--;

                        if (set.Count == 0)
                        {
                            _index.Remove(numericValue);
                            _statistics.DistinctValues = _index.Count;
                        }

                        UpdateMinMax();
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
            if (!TryConvertToDouble(value, out var numericValue))
                return EmptyLongSet.Instance;

            _lock.EnterReadLock();
            try
            {
                if (_index.TryGetValue(numericValue, out var set))
                {
                    return new HashSet<long>(set);
                }
                return EmptyLongSet.Instance;
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }

        public IReadOnlySet<long> GetRange(double min, double max, bool minInclusive = true, bool maxInclusive = false)
        {
            _lock.EnterReadLock();
            try
            {
                var result = new HashSet<long>();
                foreach (var kvp in _index)
                {
                    if (kvp.Key < min) continue;
                    if (!minInclusive && kvp.Key == min) continue;
                    if (kvp.Key > max) break;
                    if (!maxInclusive && kvp.Key == max) break;

                    foreach (var id in kvp.Value)
                    {
                        result.Add(id);
                    }
                }
                return result;
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }

        /// <summary>
        /// Get all vector IDs with values greater than the threshold.
        /// </summary>
        public IReadOnlySet<long> GetGreaterThan(double threshold, bool inclusive)
        {
            _lock.EnterReadLock();
            try
            {
                var result = new HashSet<long>();
                foreach (var kvp in _index)
                {
                    if (kvp.Key < threshold) continue;
                    if (!inclusive && kvp.Key == threshold) continue;

                    foreach (var id in kvp.Value)
                    {
                        result.Add(id);
                    }
                }
                return result;
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }

        /// <summary>
        /// Get all vector IDs with values less than the threshold.
        /// </summary>
        public IReadOnlySet<long> GetLessThan(double threshold, bool inclusive)
        {
            _lock.EnterReadLock();
            try
            {
                var result = new HashSet<long>();
                foreach (var kvp in _index)
                {
                    if (kvp.Key > threshold) break;
                    if (!inclusive && kvp.Key == threshold) continue;

                    foreach (var id in kvp.Value)
                    {
                        result.Add(id);
                    }
                }
                return result;
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }

        /// <summary>
        /// Estimate selectivity for a range predicate.
        /// Uses linear interpolation over min/max range.
        /// </summary>
        public double EstimateRangeSelectivity(double min, double max)
        {
            if (_statistics.TotalEntries == 0) return 1.0;
            if (!_statistics.MinValue.HasValue || !_statistics.MaxValue.HasValue) return 1.0;

            var indexMin = _statistics.MinValue.Value;
            var indexMax = _statistics.MaxValue.Value;

            if (indexMax <= indexMin) return 1.0;

            var rangeMin = Math.Max(min, indexMin);
            var rangeMax = Math.Min(max, indexMax);

            if (rangeMin >= rangeMax) return 0.0;

            return (rangeMax - rangeMin) / (indexMax - indexMin);
        }

        public void Clear()
        {
            _lock.EnterWriteLock();
            try
            {
                _index.Clear();
                _statistics.TotalEntries = 0;
                _statistics.DistinctValues = 0;
                _statistics.MinValue = null;
                _statistics.MaxValue = null;
            }
            finally
            {
                _lock.ExitWriteLock();
            }
        }

        private void UpdateMinMax()
        {
            if (_index.Count > 0)
            {
                _statistics.MinValue = _index.Keys.First();
                _statistics.MaxValue = _index.Keys.Last();
            }
            else
            {
                _statistics.MinValue = null;
                _statistics.MaxValue = null;
            }
        }

        private static bool TryConvertToDouble(object value, out double result)
        {
            if (value is double d) { result = d; return true; }
            if (value is int i) { result = i; return true; }
            if (value is long l) { result = l; return true; }
            if (value is float f) { result = f; return true; }
            if (value is string s && double.TryParse(s, out result)) return true;
            result = 0;
            return false;
        }

        /// <summary>
        /// Singleton empty set to avoid allocations.
        /// </summary>
        private sealed class EmptyLongSet : IReadOnlySet<long>
        {
            public static readonly EmptyLongSet Instance = new();
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
