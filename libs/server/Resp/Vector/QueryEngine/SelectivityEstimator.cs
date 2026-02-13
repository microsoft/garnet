// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

using Garnet.server.Vector.Filter;

namespace Garnet.server.Vector.QueryEngine
{
    /// <summary>
    /// Estimates the selectivity of a filter expression using index statistics.
    /// Selectivity = fraction of vectors matching the filter (0.0 to 1.0).
    /// 
    /// Uses a simple heuristic model:
    /// - Equality on indexed field: 1/cardinality
    /// - Range on indexed field: (range / total_range)
    /// - AND: selectivity_a * selectivity_b (independence assumption)
    /// - OR: selectivity_a + selectivity_b - (selectivity_a * selectivity_b)
    /// - NOT: 1 - selectivity
    /// - No index: 0.5 (assume half match — conservative fallback)
    /// </summary>
    internal sealed class SelectivityEstimator
    {
        /// <summary>
        /// Default selectivity when we can't estimate (no index, unknown expression).
        /// 0.5 biases toward post-filter, which is the safest default.
        /// </summary>
        private const double DefaultSelectivity = 0.5;

        private readonly AttributeIndexManager _indexManager;

        public SelectivityEstimator(AttributeIndexManager indexManager)
        {
            _indexManager = indexManager ?? throw new ArgumentNullException(nameof(indexManager));
        }

        /// <summary>
        /// Estimate selectivity for a filter expression.
        /// Returns value between 0.0 (matches nothing) and 1.0 (matches everything).
        /// </summary>
        public double Estimate(Expr filter)
        {
            if (filter == null) return 1.0; // No filter = everything matches

            return EstimateNode(filter);
        }

        private double EstimateNode(Expr expr)
        {
            switch (expr)
            {
                case BinaryExpr binary:
                    return EstimateBinary(binary);

                case UnaryExpr unary:
                    return EstimateUnary(unary);

                case LiteralExpr:
                case MemberExpr:
                    return DefaultSelectivity;

                default:
                    return DefaultSelectivity;
            }
        }

        private double EstimateBinary(BinaryExpr binary)
        {
            // Logical operators: combine child selectivities
            switch (binary.Operator)
            {
                case "and" or "&&":
                {
                    var leftSel = EstimateNode(binary.Left);
                    var rightSel = EstimateNode(binary.Right);
                    // Independence assumption: P(A AND B) = P(A) * P(B)
                    return leftSel * rightSel;
                }

                case "or" or "||":
                {
                    var leftSel = EstimateNode(binary.Left);
                    var rightSel = EstimateNode(binary.Right);
                    // Inclusion-exclusion: P(A OR B) = P(A) + P(B) - P(A) * P(B)
                    return leftSel + rightSel - (leftSel * rightSel);
                }
            }

            // Comparison operators: estimate from indexes
            if (binary.Left is MemberExpr member)
            {
                return EstimateComparison(member, binary.Operator, binary.Right);
            }

            // Reversed comparison: e.g., 100 < .price
            if (binary.Right is MemberExpr memberRight)
            {
                var flippedOp = FlipOperator(binary.Operator);
                if (flippedOp != null)
                {
                    return EstimateComparison(memberRight, flippedOp, binary.Left);
                }
            }

            return DefaultSelectivity;
        }

        private double EstimateComparison(MemberExpr member, string op, Expr valueExpr)
        {
            var fieldName = member.Property.StartsWith('.') ? member.Property[1..] : member.Property;
            var index = _indexManager.GetIndex(fieldName);

            // No index available - use default
            if (index == null)
                return DefaultSelectivity;

            // Try to extract literal value for estimation
            var literalValue = (valueExpr as LiteralExpr)?.Value;

            switch (op)
            {
                case "==" when index is HashIndex hashIndex:
                    return literalValue != null
                        ? hashIndex.EstimateEqualitySelectivity(literalValue)
                        : hashIndex.EstimateEqualitySelectivity();

                case "!=" when index is HashIndex hashIndex:
                    var eqSel = literalValue != null
                        ? hashIndex.EstimateEqualitySelectivity(literalValue)
                        : hashIndex.EstimateEqualitySelectivity();
                    return 1.0 - eqSel;

                case ">" or ">=" or "<" or "<=" when index is RangeIndex rangeIndex:
                    return EstimateRangeComparison(rangeIndex, op, literalValue);

                case "==" when index is RangeIndex:
                    // Equality on range index: use 1/cardinality
                    return index.Statistics.DistinctValues > 0
                        ? 1.0 / index.Statistics.DistinctValues
                        : DefaultSelectivity;

                default:
                    return DefaultSelectivity;
            }
        }

        private static double EstimateRangeComparison(RangeIndex rangeIndex, string op, object literalValue)
        {
            var stats = rangeIndex.Statistics;
            if (stats.TotalEntries == 0 || !stats.MinValue.HasValue || !stats.MaxValue.HasValue)
                return DefaultSelectivity;

            double threshold;
            if (literalValue is double d) threshold = d;
            else if (literalValue is string s && double.TryParse(s, out var parsed)) threshold = parsed;
            else return DefaultSelectivity;

            var min = stats.MinValue.Value;
            var max = stats.MaxValue.Value;
            if (max <= min) return DefaultSelectivity;

            return op switch
            {
                ">" or ">=" => Math.Clamp((max - threshold) / (max - min), 0.0, 1.0),
                "<" or "<=" => Math.Clamp((threshold - min) / (max - min), 0.0, 1.0),
                _ => DefaultSelectivity
            };
        }

        private double EstimateUnary(UnaryExpr unary)
        {
            if (unary.Operator is "not" or "!")
            {
                return 1.0 - EstimateNode(unary.Operand);
            }
            return DefaultSelectivity;
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
