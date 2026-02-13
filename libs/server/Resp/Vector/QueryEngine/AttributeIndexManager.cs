// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;

using Garnet.server.Vector.Filter;

namespace Garnet.server.Vector.QueryEngine
{
    /// <summary>
    /// Central registry for all attribute indexes on a vector set.
    /// Manages index lifecycle (create, drop, update) and provides index lookups for the query planner.
    /// 
    /// One instance per vector set, keyed by vector set name.
    /// </summary>
    internal sealed class AttributeIndexManager : IDisposable
    {
        private readonly Dictionary<string, IAttributeIndex> _indexes = new(StringComparer.OrdinalIgnoreCase);
        private readonly ReaderWriterLockSlim _lock = new();

        /// <summary>
        /// The vector set this manager belongs to.
        /// </summary>
        public string VectorSetName { get; }

        /// <summary>
        /// Total number of vectors tracked across all indexes.
        /// </summary>
        public long TotalVectors { get; private set; }

        public AttributeIndexManager(string vectorSetName)
        {
            VectorSetName = vectorSetName ?? throw new ArgumentNullException(nameof(vectorSetName));
        }

        /// <summary>
        /// Create a new attribute index on a field.
        /// </summary>
        /// <param name="fieldName">The JSON field name to index.</param>
        /// <param name="indexType">The type of index to create.</param>
        /// <returns>True if created, false if already exists.</returns>
        public bool CreateIndex(string fieldName, AttributeIndexType indexType)
        {
            _lock.EnterWriteLock();
            try
            {
                if (_indexes.ContainsKey(fieldName))
                    return false;

                IAttributeIndex index = indexType switch
                {
                    AttributeIndexType.Hash => new HashIndex(fieldName),
                    AttributeIndexType.Range => new RangeIndex(fieldName),
                    _ => throw new ArgumentException($"Unsupported index type: {indexType}")
                };

                _indexes[fieldName] = index;
                return true;
            }
            finally
            {
                _lock.ExitWriteLock();
            }
        }

        /// <summary>
        /// Drop an existing attribute index.
        /// </summary>
        /// <param name="fieldName">The field name whose index to drop.</param>
        /// <returns>True if dropped, false if not found.</returns>
        public bool DropIndex(string fieldName)
        {
            _lock.EnterWriteLock();
            try
            {
                if (_indexes.TryGetValue(fieldName, out var index))
                {
                    index.Clear();
                    _indexes.Remove(fieldName);
                    return true;
                }
                return false;
            }
            finally
            {
                _lock.ExitWriteLock();
            }
        }

        /// <summary>
        /// Get an index for a specific field, if one exists.
        /// </summary>
        public IAttributeIndex GetIndex(string fieldName)
        {
            _lock.EnterReadLock();
            try
            {
                return _indexes.TryGetValue(fieldName, out var index) ? index : null;
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }

        /// <summary>
        /// Get all available indexes.
        /// </summary>
        public IReadOnlyList<IAttributeIndex> GetAllIndexes()
        {
            _lock.EnterReadLock();
            try
            {
                return _indexes.Values.ToList();
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }

        /// <summary>
        /// Find indexes that can serve the given filter expression.
        /// Walks the AST and returns indexes matching referenced fields.
        /// </summary>
        public IReadOnlyList<IAttributeIndex> GetIndexesForFilter(Expr filter)
        {
            if (filter == null) return new List<IAttributeIndex>();

            var fields = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            CollectFieldReferences(filter, fields);

            _lock.EnterReadLock();
            try
            {
                var result = new List<IAttributeIndex>();
                foreach (var field in fields)
                {
                    // Strip leading dot from member access (e.g., ".category" -> "category")
                    var cleanField = field.StartsWith('.') ? field[1..] : field;
                    if (_indexes.TryGetValue(cleanField, out var index))
                    {
                        result.Add(index);
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
        /// Update all indexes when a vector is added or its attributes change.
        /// Parses the JSON attributes and updates each relevant index.
        /// </summary>
        /// <param name="vectorId">The internal vector ID.</param>
        /// <param name="attributeJson">The JSON attribute data.</param>
        public void OnVectorAdded(long vectorId, ReadOnlySpan<byte> attributeJson)
        {
            if (attributeJson.IsEmpty) return;

            _lock.EnterReadLock();
            try
            {
                if (_indexes.Count == 0) return;

                using var doc = JsonDocument.Parse(attributeJson.ToArray());
                var root = doc.RootElement;

                foreach (var kvp in _indexes)
                {
                    if (root.TryGetProperty(kvp.Key, out var value))
                    {
                        var indexValue = ExtractValue(value);
                        if (indexValue != null)
                        {
                            kvp.Value.Add(vectorId, indexValue);
                        }
                    }
                }

                TotalVectors++;
            }
            catch (JsonException)
            {
                // Malformed JSON - skip indexing
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }

        /// <summary>
        /// Update all indexes when a vector is removed.
        /// </summary>
        /// <param name="vectorId">The internal vector ID.</param>
        /// <param name="attributeJson">The JSON attribute data (needed to find index entries to remove).</param>
        public void OnVectorRemoved(long vectorId, ReadOnlySpan<byte> attributeJson)
        {
            if (attributeJson.IsEmpty) return;

            _lock.EnterReadLock();
            try
            {
                if (_indexes.Count == 0) return;

                using var doc = JsonDocument.Parse(attributeJson.ToArray());
                var root = doc.RootElement;

                foreach (var kvp in _indexes)
                {
                    if (root.TryGetProperty(kvp.Key, out var value))
                    {
                        var indexValue = ExtractValue(value);
                        if (indexValue != null)
                        {
                            kvp.Value.Remove(vectorId, indexValue);
                        }
                    }
                }

                TotalVectors = Math.Max(0, TotalVectors - 1);
            }
            catch (JsonException)
            {
                // Malformed JSON - skip
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }

        /// <summary>
        /// Check if any indexes exist.
        /// </summary>
        public bool HasIndexes
        {
            get
            {
                _lock.EnterReadLock();
                try { return _indexes.Count > 0; }
                finally { _lock.ExitReadLock(); }
            }
        }

        public void Dispose()
        {
            _lock.EnterWriteLock();
            try
            {
                foreach (var index in _indexes.Values)
                {
                    index.Clear();
                }
                _indexes.Clear();
            }
            finally
            {
                _lock.ExitWriteLock();
            }
            _lock.Dispose();
        }

        /// <summary>
        /// Recursively collect field names referenced in the filter expression.
        /// </summary>
        private static void CollectFieldReferences(Expr expr, HashSet<string> fields)
        {
            switch (expr)
            {
                case MemberExpr member:
                    fields.Add(member.Property);
                    break;
                case BinaryExpr binary:
                    CollectFieldReferences(binary.Left, fields);
                    CollectFieldReferences(binary.Right, fields);
                    break;
                case UnaryExpr unary:
                    CollectFieldReferences(unary.Operand, fields);
                    break;
            }
        }

        /// <summary>
        /// Extract a value from a JsonElement suitable for indexing.
        /// </summary>
        private static object ExtractValue(JsonElement element)
        {
            return element.ValueKind switch
            {
                JsonValueKind.String => element.GetString(),
                JsonValueKind.Number => element.GetDouble(),
                JsonValueKind.True => "true",
                JsonValueKind.False => "false",
                _ => null
            };
        }
    }
}
