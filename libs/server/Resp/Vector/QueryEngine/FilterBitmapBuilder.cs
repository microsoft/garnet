// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;

using Garnet.server.Vector.Filter;

namespace Garnet.server.Vector.QueryEngine
{
    /// <summary>
    /// Builds a <see cref="FilterBitmap"/> from a filter expression by querying attribute indexes.
    /// Supports AND/OR/NOT composition of index query results.
    /// 
    /// For predicates without an available index, the bitmap is set to all-1s (match everything)
    /// and the predicate falls through to post-filtering as a safety net.
    /// </summary>
    internal sealed class FilterBitmapBuilder
    {
        private readonly AttributeIndexManager _indexManager;
        private readonly long _totalVectors;

        /// <summary>
        /// Tracks which predicates could NOT be resolved via indexes
        /// and need post-filter evaluation.
        /// </summary>
        public bool HasUnresolvedPredicates { get; private set; }

        public FilterBitmapBuilder(AttributeIndexManager indexManager, long totalVectors)
        {
            _indexManager = indexManager ?? throw new ArgumentNullException(nameof(indexManager));
            _totalVectors = totalVectors;
        }

        /// <summary>
        /// Build a bitmap from a filter expression.
        /// Each bit represents whether a vector ID passes the filter.
        /// </summary>
        /// <returns>
        /// A bitmap where set bits indicate vectors that match indexed predicates.
        /// If <see cref="HasUnresolvedPredicates"/> is true, post-filtering should still
        /// be applied for non-indexed predicates.
        /// </returns>
        public FilterBitmap Build(Expr filter)
        {
            HasUnresolvedPredicates = false;

            if (filter == null)
            {
                // No filter - everything matches
                return CreateAllSetBitmap();
            }

            return BuildNode(filter);
        }

        private FilterBitmap BuildNode(Expr expr)
        {
            switch (expr)
            {
                case BinaryExpr binary:
                    return BuildBinary(binary);

                case UnaryExpr unary:
                    return BuildUnary(unary);

                default:
                    // Unknown expression type — can't resolve with index
                    HasUnresolvedPredicates = true;
                    return CreateAllSetBitmap();
            }
        }

        private FilterBitmap BuildBinary(BinaryExpr binary)
        {
            // Logical operators: combine child bitmaps
            switch (binary.Operator)
            {
                case "and" or "&&":
                {
                    var left = BuildNode(binary.Left);
                    var right = BuildNode(binary.Right);
                    return FilterBitmap.And(left, right);
                }

                case "or" or "||":
                {
                    var left = BuildNode(binary.Left);
                    var right = BuildNode(binary.Right);
                    return FilterBitmap.Or(left, right);
                }
            }

            // Comparison operators: query index
            if (binary.Left is MemberExpr member && binary.Right is LiteralExpr literal)
            {
                return BuildComparison(member, binary.Operator, literal.Value);
            }

            // Reversed comparison: e.g., 100 < .price → .price > 100
            if (binary.Left is LiteralExpr litLeft && binary.Right is MemberExpr memberRight)
            {
                var flippedOp = FlipOperator(binary.Operator);
                if (flippedOp != null)
                {
                    return BuildComparison(memberRight, flippedOp, litLeft.Value);
                }
            }

            // Can't resolve this predicate with indexes
            HasUnresolvedPredicates = true;
            return CreateAllSetBitmap();
        }

        private FilterBitmap BuildComparison(MemberExpr member, string op, object value)
        {
            var fieldName = member.Property.StartsWith('.') ? member.Property[1..] : member.Property;
            var index = _indexManager.GetIndex(fieldName);

            if (index == null)
            {
                // No index for this field — mark as unresolved
                HasUnresolvedPredicates = true;
                return CreateAllSetBitmap();
            }

            IReadOnlySet<long> matchingIds;

            switch (op)
            {
                case "==":
                    matchingIds = index.GetEqual(value);
                    break;

                case "!=":
                {
                    var equalIds = index.GetEqual(value);
                    var bitmap = FilterBitmap.FromIds(equalIds, _totalVectors);
                    return FilterBitmap.Not(bitmap, _totalVectors);
                }

                case ">" when index is RangeIndex rangeIndex:
                    matchingIds = rangeIndex.GetGreaterThan(ToDouble(value), inclusive: false);
                    break;

                case ">=" when index is RangeIndex rangeIndex:
                    matchingIds = rangeIndex.GetGreaterThan(ToDouble(value), inclusive: true);
                    break;

                case "<" when index is RangeIndex rangeIndex:
                    matchingIds = rangeIndex.GetLessThan(ToDouble(value), inclusive: false);
                    break;

                case "<=" when index is RangeIndex rangeIndex:
                    matchingIds = rangeIndex.GetLessThan(ToDouble(value), inclusive: true);
                    break;

                default:
                    // Operator not supported by this index type
                    HasUnresolvedPredicates = true;
                    return CreateAllSetBitmap();
            }

            return FilterBitmap.FromIds(matchingIds, _totalVectors);
        }

        private FilterBitmap BuildUnary(UnaryExpr unary)
        {
            if (unary.Operator is "not" or "!")
            {
                var child = BuildNode(unary.Operand);
                return FilterBitmap.Not(child, _totalVectors);
            }

            HasUnresolvedPredicates = true;
            return CreateAllSetBitmap();
        }

        private FilterBitmap CreateAllSetBitmap()
        {
            var bitmap = new FilterBitmap(_totalVectors);
            for (long i = 0; i < _totalVectors; i++)
            {
                bitmap.Set(i);
            }
            return bitmap;
        }

        private static double ToDouble(object value)
        {
            if (value is double d) return d;
            if (value is int i) return i;
            if (value is long l) return l;
            if (value is string s && double.TryParse(s, out var result)) return result;
            return 0;
        }

        private static string FlipOperator(string op)
        {
            return op switch
            {
                ">" => "<",
                "<" => ">",
                ">=" => "<=",
                "<=" => ">=",
                "==" => "==",
                "!=" => "!=",
                _ => null
            };
        }
    }
}
