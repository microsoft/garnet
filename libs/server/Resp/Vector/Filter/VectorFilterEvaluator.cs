// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Text.Json;

namespace Garnet.server.Vector.Filter
{
    /// <summary>
    /// Evaluator for vector filter expressions.
    /// Evaluates parsed expression trees against JSON attribute data.
    /// </summary>
    internal static class VectorFilterEvaluator
    {
        public static object EvaluateExpression(Expr expr, JsonElement root)
        {
            if (expr is LiteralExpr lit)
                return lit.Value;

            if (expr is MemberExpr member)
            {
                if (root.TryGetProperty(member.Property, out var value))
                {
                    return value.ValueKind switch
                    {
                        JsonValueKind.Number => value.GetDouble(),
                        JsonValueKind.String => value.GetString(),
                        JsonValueKind.True => 1.0,
                        JsonValueKind.False => 0.0,
                        JsonValueKind.Array => value,
                        _ => null
                    };
                }
                return null;
            }

            if (expr is UnaryExpr unary)
            {
                var operand = EvaluateExpression(unary.Operand, root);
                if (unary.Operator == "not" || unary.Operator == "!")
                    return IsTruthy(operand) ? 0.0 : 1.0;
                if (unary.Operator == "-")
                    return -(ToNumber(operand));
            }

            if (expr is BinaryExpr binary)
            {
                var left = EvaluateExpression(binary.Left, root);
                var right = EvaluateExpression(binary.Right, root);

                return binary.Operator switch
                {
                    "+" => ToNumber(left) + ToNumber(right),
                    "-" => ToNumber(left) - ToNumber(right),
                    "*" => ToNumber(left) * ToNumber(right),
                    "/" => ToNumber(left) / ToNumber(right),
                    "%" => ToNumber(left) % ToNumber(right),
                    "**" => Math.Pow(ToNumber(left), ToNumber(right)),
                    ">" => ToNumber(left) > ToNumber(right) ? 1.0 : 0.0,
                    "<" => ToNumber(left) < ToNumber(right) ? 1.0 : 0.0,
                    ">=" => ToNumber(left) >= ToNumber(right) ? 1.0 : 0.0,
                    "<=" => ToNumber(left) <= ToNumber(right) ? 1.0 : 0.0,
                    "==" => AreEqual(left, right) ? 1.0 : 0.0,
                    "!=" => !AreEqual(left, right) ? 1.0 : 0.0,
                    "and" or "&&" => IsTruthy(left) && IsTruthy(right) ? 1.0 : 0.0,
                    "or" or "||" => IsTruthy(left) || IsTruthy(right) ? 1.0 : 0.0,
                    "in" => IsIn(left, right) ? 1.0 : 0.0,
                    _ => throw new InvalidOperationException($"Unknown operator: {binary.Operator}")
                };
            }

            return null;
        }

        private static double ToNumber(object value)
        {
            if (value is double d) return d;
            if (value is int i) return i;
            if (value is string s && double.TryParse(s, out var result)) return result;
            return 0;
        }

        public static bool IsTruthy(object value)
        {
            if (value == null) return false;
            if (value is double d) return d != 0;
            if (value is int i) return i != 0;
            if (value is string s) return !string.IsNullOrEmpty(s);
            if (value is bool b) return b;
            return true;
        }

        private static bool AreEqual(object left, object right)
        {
            if (left == null && right == null) return true;
            if (left == null || right == null) return false;

            if (left is double || right is double)
                return Math.Abs(ToNumber(left) - ToNumber(right)) < 0.0001;

            if (left is string ls && right is string rs)
                return ls == rs;

            return left.Equals(right);
        }

        private static bool IsIn(object needle, object haystack)
        {
            if (haystack is JsonElement elem && elem.ValueKind == JsonValueKind.Array)
            {
                foreach (var item in elem.EnumerateArray())
                {
                    var itemValue = item.ValueKind switch
                    {
                        JsonValueKind.Number => (object)item.GetDouble(),
                        JsonValueKind.String => item.GetString(),
                        JsonValueKind.True => 1.0,
                        JsonValueKind.False => 0.0,
                        _ => null
                    };

                    if (AreEqual(needle, itemValue))
                        return true;
                }
            }
            return false;
        }
    }
}
