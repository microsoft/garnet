// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using System.Globalization;

namespace Garnet.server.Vector.Filter
{
    /// <summary>
    /// Recursive descent parser for vector filter expressions.
    /// Supports arithmetic, comparison, logical operators, containment, and grouping.
    /// Uses TryParse pattern to avoid exceptions in the hot path.
    /// Includes a recursion depth guard to prevent stack overflow from deeply nested expressions.
    /// </summary>
    internal static class VectorFilterParser
    {
        /// <summary>
        /// Maximum recursion depth allowed during parsing to prevent stack overflow.
        /// This limit is intentionally conservative; typical filter expressions are shallow.
        /// Windows and Linux may use different default stack sizes, so we keep this low.
        /// </summary>
        private const int MaxRecursionDepth = 64;

        /// <summary>
        /// Attempt to parse a filter expression from the token list.
        /// Returns false with an error message if parsing fails.
        /// </summary>
        /// <param name="tokens">The list of tokens to parse.</param>
        /// <param name="start">The starting token index.</param>
        /// <param name="result">The parsed expression tree, or null on failure.</param>
        /// <param name="end">The index past the last consumed token.</param>
        /// <param name="error">An error message describing the failure, or null on success.</param>
        /// <returns>True if parsing succeeded; false otherwise.</returns>
        public static bool TryParseExpression(List<Token> tokens, int start, out Expr result, out int end, out string error)
        {
            return TryParseLogicalOr(tokens, start, out result, out end, out error, depth: 0);
        }

        private static bool TryParseLogicalOr(List<Token> tokens, int start, out Expr result, out int end, out string error, int depth)
        {
            if (!TryParseLogicalAnd(tokens, start, out var left, out end, out error, depth))
            {
                result = null;
                return false;
            }

            while (end < tokens.Count &&
                   ((tokens[end].Type == TokenType.Keyword && tokens[end].Value == "or") ||
                    (tokens[end].Type == TokenType.Operator && tokens[end].Value == "||")))
            {
                end++;
                if (!TryParseLogicalAnd(tokens, end, out var right, out end, out error, depth))
                {
                    result = null;
                    return false;
                }
                left = new BinaryExpr { Left = left, Operator = OperatorKind.Or, Right = right };
            }

            result = left;
            return true;
        }

        private static bool TryParseLogicalAnd(List<Token> tokens, int start, out Expr result, out int end, out string error, int depth)
        {
            if (!TryParseEquality(tokens, start, out var left, out end, out error, depth))
            {
                result = null;
                return false;
            }

            while (end < tokens.Count &&
                   ((tokens[end].Type == TokenType.Keyword && tokens[end].Value == "and") ||
                    (tokens[end].Type == TokenType.Operator && tokens[end].Value == "&&")))
            {
                end++;
                if (!TryParseEquality(tokens, end, out var right, out end, out error, depth))
                {
                    result = null;
                    return false;
                }
                left = new BinaryExpr { Left = left, Operator = OperatorKind.And, Right = right };
            }

            result = left;
            return true;
        }

        private static bool TryParseEquality(List<Token> tokens, int start, out Expr result, out int end, out string error, int depth)
        {
            if (!TryParseComparison(tokens, start, out var left, out end, out error, depth))
            {
                result = null;
                return false;
            }

            while (end < tokens.Count && tokens[end].Type == TokenType.Operator &&
                   (tokens[end].Value == "==" || tokens[end].Value == "!="))
            {
                var op = tokens[end].Value == "==" ? OperatorKind.Equal : OperatorKind.NotEqual;
                end++;
                if (!TryParseComparison(tokens, end, out var right, out end, out error, depth))
                {
                    result = null;
                    return false;
                }
                left = new BinaryExpr { Left = left, Operator = op, Right = right };
            }

            result = left;
            return true;
        }

        private static bool TryParseComparison(List<Token> tokens, int start, out Expr result, out int end, out string error, int depth)
        {
            if (!TryParseContainment(tokens, start, out var left, out end, out error, depth))
            {
                result = null;
                return false;
            }

            while (end < tokens.Count && tokens[end].Type == TokenType.Operator &&
                   (tokens[end].Value == ">" || tokens[end].Value == "<" ||
                    tokens[end].Value == ">=" || tokens[end].Value == "<="))
            {
                var op = ParseComparisonOperator(tokens[end].Value);
                end++;
                if (!TryParseContainment(tokens, end, out var right, out end, out error, depth))
                {
                    result = null;
                    return false;
                }
                left = new BinaryExpr { Left = left, Operator = op, Right = right };
            }

            result = left;
            return true;
        }

        private static OperatorKind ParseComparisonOperator(string value)
        {
            // Length check first for fast disambiguation
            if (value.Length == 1)
                return value[0] == '>' ? OperatorKind.GreaterThan : OperatorKind.LessThan;
            return value[0] == '>' ? OperatorKind.GreaterEqual : OperatorKind.LessEqual;
        }

        private static bool TryParseContainment(List<Token> tokens, int start, out Expr result, out int end, out string error, int depth)
        {
            if (!TryParseAdditive(tokens, start, out var left, out end, out error, depth))
            {
                result = null;
                return false;
            }

            if (end < tokens.Count && tokens[end].Type == TokenType.Keyword && tokens[end].Value == "in")
            {
                end++;
                if (!TryParseAdditive(tokens, end, out var right, out end, out error, depth))
                {
                    result = null;
                    return false;
                }
                left = new BinaryExpr { Left = left, Operator = OperatorKind.In, Right = right };
            }

            result = left;
            return true;
        }

        private static bool TryParseAdditive(List<Token> tokens, int start, out Expr result, out int end, out string error, int depth)
        {
            if (!TryParseMultiplicative(tokens, start, out var left, out end, out error, depth))
            {
                result = null;
                return false;
            }

            while (end < tokens.Count && tokens[end].Type == TokenType.Operator &&
                   (tokens[end].Value == "+" || tokens[end].Value == "-"))
            {
                var op = tokens[end].Value == "+" ? OperatorKind.Add : OperatorKind.Subtract;
                end++;
                if (!TryParseMultiplicative(tokens, end, out var right, out end, out error, depth))
                {
                    result = null;
                    return false;
                }
                left = new BinaryExpr { Left = left, Operator = op, Right = right };
            }

            result = left;
            return true;
        }

        private static bool TryParseMultiplicative(List<Token> tokens, int start, out Expr result, out int end, out string error, int depth)
        {
            if (!TryParseExponentiation(tokens, start, out var left, out end, out error, depth))
            {
                result = null;
                return false;
            }

            while (end < tokens.Count && tokens[end].Type == TokenType.Operator &&
                   (tokens[end].Value == "*" || tokens[end].Value == "/" || tokens[end].Value == "%"))
            {
                var op = tokens[end].Value[0] switch
                {
                    '*' => OperatorKind.Multiply,
                    '/' => OperatorKind.Divide,
                    _ => OperatorKind.Modulo
                };
                end++;
                if (!TryParseExponentiation(tokens, end, out var right, out end, out error, depth))
                {
                    result = null;
                    return false;
                }
                left = new BinaryExpr { Left = left, Operator = op, Right = right };
            }

            result = left;
            return true;
        }

        private static bool TryParseExponentiation(List<Token> tokens, int start, out Expr result, out int end, out string error, int depth)
        {
            if (!TryParseUnary(tokens, start, out var left, out end, out error, depth))
            {
                result = null;
                return false;
            }

            if (end < tokens.Count && tokens[end].Type == TokenType.Operator && tokens[end].Value == "**")
            {
                end++;
                // Right associative — recurse into exponentiation
                if (!TryParseExponentiation(tokens, end, out var right, out end, out error, depth))
                {
                    result = null;
                    return false;
                }
                left = new BinaryExpr { Left = left, Operator = OperatorKind.Power, Right = right };
            }

            result = left;
            return true;
        }

        private static bool TryParseUnary(List<Token> tokens, int start, out Expr result, out int end, out string error, int depth)
        {
            if (start < tokens.Count)
            {
                if ((tokens[start].Type == TokenType.Keyword && tokens[start].Value == "not") ||
                    (tokens[start].Type == TokenType.Operator && tokens[start].Value == "!"))
                {
                    start++;
                    if (!TryParseUnary(tokens, start, out var operand, out end, out error, depth))
                    {
                        result = null;
                        return false;
                    }
                    result = new UnaryExpr { Operator = OperatorKind.Not, Operand = operand };
                    return true;
                }

                if (tokens[start].Type == TokenType.Operator && tokens[start].Value == "-")
                {
                    start++;
                    if (!TryParseUnary(tokens, start, out var operand, out end, out error, depth))
                    {
                        result = null;
                        return false;
                    }
                    result = new UnaryExpr { Operator = OperatorKind.Negate, Operand = operand };
                    return true;
                }
            }

            return TryParsePrimary(tokens, start, out result, out end, out error, depth);
        }

        private static bool TryParsePrimary(List<Token> tokens, int start, out Expr result, out int end, out string error, int depth)
        {
            result = null;

            if (start >= tokens.Count)
            {
                end = start;
                error = "Unexpected end of expression";
                return false;
            }

            var token = tokens[start];

            // Parentheses — increase recursion depth
            if (token.Type == TokenType.Delimiter && token.Value == "(")
            {
                var newDepth = depth + 1;
                if (newDepth > MaxRecursionDepth)
                {
                    end = start;
                    error = $"Filter expression exceeds maximum nesting depth of {MaxRecursionDepth}";
                    return false;
                }

                if (!TryParseLogicalOr(tokens, start + 1, out var expr, out end, out error, newDepth))
                    return false;

                if (end >= tokens.Count || tokens[end].Type != TokenType.Delimiter || tokens[end].Value != ")")
                {
                    error = "Missing closing parenthesis";
                    return false;
                }
                end++;
                result = expr;
                return true;
            }

            // Literals — use FilterValue to avoid boxing doubles
            if (token.Type == TokenType.Number)
            {
                if (!double.TryParse(token.Value, NumberStyles.Float | NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out var numValue))
                {
                    end = start;
                    error = $"Invalid number literal: {token.Value}";
                    return false;
                }
                end = start + 1;
                result = new LiteralExpr { Value = FilterValue.FromNumber(numValue) };
                error = null;
                return true;
            }

            if (token.Type == TokenType.String)
            {
                end = start + 1;
                result = new LiteralExpr { Value = FilterValue.FromString(token.Value) };
                error = null;
                return true;
            }

            if (token.Type == TokenType.Boolean)
            {
                end = start + 1;
                result = new LiteralExpr { Value = token.Value == "true" ? FilterValue.True : FilterValue.False };
                error = null;
                return true;
            }

            // Identifier (field access)
            if (token.Type == TokenType.Identifier)
            {
                end = start + 1;
                result = new MemberExpr { Property = token.Value.TrimStart('.') };
                error = null;
                return true;
            }

            end = start;
            error = $"Unexpected token: {token.Value}";
            return false;
        }
    }
}