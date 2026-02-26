// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text.Json;

namespace Garnet.server.Vector.Filter
{
    /// <summary>
    /// Evaluator for vector filter expressions.
    /// Evaluates parsed expression trees against JSON attribute data.
    /// Returns FilterValue (a struct) to avoid boxing allocations on every evaluation.
    ///
    /// Note: This evaluator operates over top-level properties of the JSON document only.
    /// Nested property access is not supported. A future optimization could replace the
    /// JsonElement-based lookup with a raw span + (offset, length) pairs approach for
    /// better performance, avoiding JsonDocument allocation entirely.
    /// </summary>
    internal static class VectorFilterEvaluator
    {
        /// <summary>
        /// Evaluate a filter expression against a JSON element and return a boolean result.
        /// This is the primary public API for filter evaluation.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool EvaluateFilterBool(Expr expr, JsonElement root)
        {
            return IsTruthy(EvaluateExpression(expr, root));
        }

        /// <summary>
        /// Evaluate a filter expression against a JSON element.
        /// Returns a FilterValue (struct) — no boxing occurs for numeric results.
        /// </summary>
        public static FilterValue EvaluateExpression(Expr expr, JsonElement root)
        {
            if (expr is LiteralExpr lit)
                return lit.Value;

            if (expr is MemberExpr member)
                return EvaluateMember(member, root);

            if (expr is UnaryExpr unary)
                return EvaluateUnary(unary, root);

            if (expr is BinaryExpr binary)
                return EvaluateBinary(binary, root);

            return FilterValue.Null;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static FilterValue EvaluateMember(MemberExpr member, JsonElement root)
        {
            if (root.TryGetProperty(member.Property, out var value))
            {
                return value.ValueKind switch
                {
                    JsonValueKind.Number => FilterValue.FromNumber(value.GetDouble()),
                    JsonValueKind.String => FilterValue.FromString(value.GetString()),
                    JsonValueKind.True => FilterValue.True,
                    JsonValueKind.False => FilterValue.False,
                    JsonValueKind.Array => FilterValue.FromJsonElement(value),
                    _ => FilterValue.Null
                };
            }
            return FilterValue.Null;
        }

        private static FilterValue EvaluateUnary(UnaryExpr unary, JsonElement root)
        {
            var operand = EvaluateExpression(unary.Operand, root);
            return unary.Operator switch
            {
                OperatorKind.Not => IsTruthy(operand) ? FilterValue.False : FilterValue.True,
                OperatorKind.Negate => FilterValue.FromNumber(-ToNumber(operand)),
                _ => FilterValue.Null
            };
        }

        private static FilterValue EvaluateBinary(BinaryExpr binary, JsonElement root)
        {
            // Short-circuit logical operators
            if (binary.Operator == OperatorKind.And)
            {
                var left = EvaluateExpression(binary.Left, root);
                if (!IsTruthy(left)) return FilterValue.False;
                var right = EvaluateExpression(binary.Right, root);
                return IsTruthy(right) ? FilterValue.True : FilterValue.False;
            }

            if (binary.Operator == OperatorKind.Or)
            {
                var left = EvaluateExpression(binary.Left, root);
                if (IsTruthy(left)) return FilterValue.True;
                var right = EvaluateExpression(binary.Right, root);
                return IsTruthy(right) ? FilterValue.True : FilterValue.False;
            }

            {
                var left = EvaluateExpression(binary.Left, root);
                var right = EvaluateExpression(binary.Right, root);

                return binary.Operator switch
                {
                    OperatorKind.Add => FilterValue.FromNumber(ToNumber(left) + ToNumber(right)),
                    OperatorKind.Subtract => FilterValue.FromNumber(ToNumber(left) - ToNumber(right)),
                    OperatorKind.Multiply => FilterValue.FromNumber(ToNumber(left) * ToNumber(right)),
                    OperatorKind.Divide => FilterValue.FromNumber(ToNumber(left) / ToNumber(right)),
                    OperatorKind.Modulo => FilterValue.FromNumber(ToNumber(left) % ToNumber(right)),
                    OperatorKind.Power => FilterValue.FromNumber(Math.Pow(ToNumber(left), ToNumber(right))),
                    OperatorKind.GreaterThan => FilterValue.FromBool(ToNumber(left) > ToNumber(right)),
                    OperatorKind.LessThan => FilterValue.FromBool(ToNumber(left) < ToNumber(right)),
                    OperatorKind.GreaterEqual => FilterValue.FromBool(ToNumber(left) >= ToNumber(right)),
                    OperatorKind.LessEqual => FilterValue.FromBool(ToNumber(left) <= ToNumber(right)),
                    OperatorKind.Equal => FilterValue.FromBool(AreEqual(left, right)),
                    OperatorKind.NotEqual => FilterValue.FromBool(!AreEqual(left, right)),
                    OperatorKind.In => FilterValue.FromBool(IsIn(left, right)),
                    _ => FilterValue.Null
                };
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static double ToNumber(FilterValue value)
        {
            return value.Kind switch
            {
                FilterValueKind.Number => value.AsNumber(),
                FilterValueKind.String => double.TryParse(value.AsString(), NumberStyles.Float | NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out var result) ? result : 0,
                _ => 0
            };
        }

        /// <summary>
        /// Determine if a FilterValue is truthy.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsTruthy(FilterValue value)
        {
            return value.Kind switch
            {
                FilterValueKind.Number => value.AsNumber() != 0,
                FilterValueKind.String => !string.IsNullOrEmpty(value.AsString()),
                FilterValueKind.Null => false,
                _ => true  // JsonArray etc. are truthy
            };
        }

        private static bool AreEqual(FilterValue left, FilterValue right)
        {
            if (left.IsNull && right.IsNull) return true;
            if (left.IsNull || right.IsNull) return false;

            // Both are numbers — fast numeric comparison
            if (left.Kind == FilterValueKind.Number && right.Kind == FilterValueKind.Number)
                return Math.Abs(left.AsNumber() - right.AsNumber()) < 0.0001;

            // If either is a number and the other might be convertible
            if (left.Kind == FilterValueKind.Number || right.Kind == FilterValueKind.Number)
                return Math.Abs(ToNumber(left) - ToNumber(right)) < 0.0001;

            // Both are strings
            if (left.Kind == FilterValueKind.String && right.Kind == FilterValueKind.String)
                return left.AsString() == right.AsString();

            return false;
        }

        private static bool IsIn(FilterValue needle, FilterValue haystack)
        {
            if (haystack.Kind == FilterValueKind.JsonArray)
            {
                var elem = haystack.AsJsonElement();
                if (elem.ValueKind == JsonValueKind.Array)
                {
                    foreach (var item in elem.EnumerateArray())
                    {
                        var itemValue = item.ValueKind switch
                        {
                            JsonValueKind.Number => FilterValue.FromNumber(item.GetDouble()),
                            JsonValueKind.String => FilterValue.FromString(item.GetString()),
                            JsonValueKind.True => FilterValue.True,
                            JsonValueKind.False => FilterValue.False,
                            _ => FilterValue.Null
                        };

                        if (AreEqual(needle, itemValue))
                            return true;
                    }
                }
            }
            return false;
        }
    }
}