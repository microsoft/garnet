// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;

namespace Garnet.server.Vector.Filter
{
    /// <summary>
    /// Recursive descent parser for vector filter expressions.
    /// Supports arithmetic, comparison, logical operators, containment, and grouping.
    /// </summary>
    internal static class VectorFilterParser
    {
        public static Expr ParseExpression(List<Token> tokens, int start, out int end)
        {
            return ParseLogicalOr(tokens, start, out end);
        }

        private static Expr ParseLogicalOr(List<Token> tokens, int start, out int end)
        {
            var left = ParseLogicalAnd(tokens, start, out end);

            while (end < tokens.Count &&
                   ((tokens[end].Type == TokenType.Keyword && tokens[end].Value == "or") ||
                    (tokens[end].Type == TokenType.Operator && tokens[end].Value == "||")))
            {
                end++;
                var right = ParseLogicalAnd(tokens, end, out end);
                left = new BinaryExpr { Left = left, Operator = "or", Right = right };
            }

            return left;
        }

        private static Expr ParseLogicalAnd(List<Token> tokens, int start, out int end)
        {
            var left = ParseEquality(tokens, start, out end);

            while (end < tokens.Count &&
                   ((tokens[end].Type == TokenType.Keyword && tokens[end].Value == "and") ||
                    (tokens[end].Type == TokenType.Operator && tokens[end].Value == "&&")))
            {
                end++;
                var right = ParseEquality(tokens, end, out end);
                left = new BinaryExpr { Left = left, Operator = "and", Right = right };
            }

            return left;
        }

        private static Expr ParseEquality(List<Token> tokens, int start, out int end)
        {
            var left = ParseComparison(tokens, start, out end);

            while (end < tokens.Count && tokens[end].Type == TokenType.Operator &&
                   (tokens[end].Value == "==" || tokens[end].Value == "!="))
            {
                var op = tokens[end].Value;
                end++;
                var right = ParseComparison(tokens, end, out end);
                left = new BinaryExpr { Left = left, Operator = op, Right = right };
            }

            return left;
        }

        private static Expr ParseComparison(List<Token> tokens, int start, out int end)
        {
            var left = ParseContainment(tokens, start, out end);

            while (end < tokens.Count && tokens[end].Type == TokenType.Operator &&
                   (tokens[end].Value == ">" || tokens[end].Value == "<" ||
                    tokens[end].Value == ">=" || tokens[end].Value == "<="))
            {
                var op = tokens[end].Value;
                end++;
                var right = ParseContainment(tokens, end, out end);
                left = new BinaryExpr { Left = left, Operator = op, Right = right };
            }

            return left;
        }

        private static Expr ParseContainment(List<Token> tokens, int start, out int end)
        {
            var left = ParseAdditive(tokens, start, out end);

            if (end < tokens.Count && tokens[end].Type == TokenType.Keyword && tokens[end].Value == "in")
            {
                end++;
                var right = ParseAdditive(tokens, end, out end);
                left = new BinaryExpr { Left = left, Operator = "in", Right = right };
            }

            return left;
        }

        private static Expr ParseAdditive(List<Token> tokens, int start, out int end)
        {
            var left = ParseMultiplicative(tokens, start, out end);

            while (end < tokens.Count && tokens[end].Type == TokenType.Operator &&
                   (tokens[end].Value == "+" || tokens[end].Value == "-"))
            {
                var op = tokens[end].Value;
                end++;
                var right = ParseMultiplicative(tokens, end, out end);
                left = new BinaryExpr { Left = left, Operator = op, Right = right };
            }

            return left;
        }

        private static Expr ParseMultiplicative(List<Token> tokens, int start, out int end)
        {
            var left = ParseExponentiation(tokens, start, out end);

            while (end < tokens.Count && tokens[end].Type == TokenType.Operator &&
                   (tokens[end].Value == "*" || tokens[end].Value == "/" || tokens[end].Value == "%"))
            {
                var op = tokens[end].Value;
                end++;
                var right = ParseExponentiation(tokens, end, out end);
                left = new BinaryExpr { Left = left, Operator = op, Right = right };
            }

            return left;
        }

        private static Expr ParseExponentiation(List<Token> tokens, int start, out int end)
        {
            var left = ParseUnary(tokens, start, out end);

            if (end < tokens.Count && tokens[end].Type == TokenType.Operator && tokens[end].Value == "**")
            {
                end++;
                var right = ParseExponentiation(tokens, end, out end); // Right associative
                left = new BinaryExpr { Left = left, Operator = "**", Right = right };
            }

            return left;
        }

        private static Expr ParseUnary(List<Token> tokens, int start, out int end)
        {
            if (start < tokens.Count)
            {
                if ((tokens[start].Type == TokenType.Keyword && tokens[start].Value == "not") ||
                    (tokens[start].Type == TokenType.Operator && tokens[start].Value == "!"))
                {
                    start++;
                    var operand = ParseUnary(tokens, start, out end);
                    return new UnaryExpr { Operator = "not", Operand = operand };
                }

                if (tokens[start].Type == TokenType.Operator && tokens[start].Value == "-")
                {
                    start++;
                    var operand = ParseUnary(tokens, start, out end);
                    return new UnaryExpr { Operator = "-", Operand = operand };
                }
            }

            return ParsePrimary(tokens, start, out end);
        }

        private static Expr ParsePrimary(List<Token> tokens, int start, out int end)
        {
            if (start >= tokens.Count)
                throw new InvalidOperationException("Unexpected end of expression");

            var token = tokens[start];

            // Parentheses
            if (token.Type == TokenType.Delimiter && token.Value == "(")
            {
                var expr = ParseExpression(tokens, start + 1, out end);
                if (end >= tokens.Count || tokens[end].Type != TokenType.Delimiter || tokens[end].Value != ")")
                    throw new InvalidOperationException("Missing closing parenthesis");
                end++;
                return expr;
            }

            // Literals
            if (token.Type == TokenType.Number)
            {
                end = start + 1;
                return new LiteralExpr { Value = double.Parse(token.Value) };
            }

            if (token.Type == TokenType.String)
            {
                end = start + 1;
                return new LiteralExpr { Value = token.Value };
            }

            if (token.Type == TokenType.Boolean)
            {
                end = start + 1;
                return new LiteralExpr { Value = token.Value == "true" ? 1.0 : 0.0 };
            }

            // Identifier (field access)
            if (token.Type == TokenType.Identifier)
            {
                end = start + 1;
                return new MemberExpr { Property = token.Value.TrimStart('.') };
            }

            throw new InvalidOperationException($"Unexpected token: {token.Value}");
        }
    }
}
