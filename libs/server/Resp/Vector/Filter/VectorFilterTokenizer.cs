// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Garnet.server.Vector.Filter
{
    /// <summary>
    /// Token types for vector filter expressions.
    /// </summary>
    internal enum TokenType : byte { Number, String, Boolean, Identifier, Operator, Keyword, Delimiter }

    /// <summary>
    /// Represents a token in a filter expression.
    /// Value type to avoid per-token heap allocations.
    /// </summary>
    internal readonly struct Token
    {
        public Token(TokenType type, string value)
        {
            Type = type;
            Value = value;
        }

        public TokenType Type { get; }
        public string Value { get; }
    }

    /// <summary>
    /// Tokenizer for vector filter expressions.
    /// Converts filter strings into tokens for parsing.
    /// </summary>
    internal static class VectorFilterTokenizer
    {
        // Pre-cached operator strings to avoid per-token string allocations
        private const string OpPlus = "+";
        private const string OpMinus = "-";
        private const string OpStar = "*";
        private const string OpSlash = "/";
        private const string OpPercent = "%";
        private const string OpGreater = ">";
        private const string OpLess = "<";
        private const string OpBang = "!";
        private const string OpOpenParen = "(";
        private const string OpCloseParen = ")";

        private const string OpEqualEqual = "==";
        private const string OpBangEqual = "!=";
        private const string OpGreaterEqual = ">=";
        private const string OpLessEqual = "<=";
        private const string OpAmpAmp = "&&";
        private const string OpPipePipe = "||";
        private const string OpStarStar = "**";

        public static List<Token> Tokenize(string input)
        {
            var tokens = new List<Token>();
            var i = 0;

            while (i < input.Length)
            {
                // Skip whitespace
                if (char.IsWhiteSpace(input[i]))
                {
                    i++;
                    continue;
                }

                // Numbers (treat '-' as negative sign only at start or after operator/keyword/open-paren)
                var isNegativeNumber = input[i] == '-'
                    && i + 1 < input.Length && char.IsDigit(input[i + 1])
                    && (tokens.Count == 0
                        || tokens[^1].Type == TokenType.Operator
                        || tokens[^1].Type == TokenType.Keyword
                        || (tokens[^1].Type == TokenType.Delimiter && tokens[^1].Value == OpOpenParen));

                if (char.IsDigit(input[i]) || isNegativeNumber)
                {
                    var start = i;
                    if (input[i] == '-') i++;
                    while (i < input.Length && (char.IsDigit(input[i]) || input[i] == '.'))
                        i++;
                    tokens.Add(new Token(TokenType.Number, input.Substring(start, i - start)));
                    continue;
                }

                // Identifiers and keywords (field names starting with .)
                if (input[i] == '.' || char.IsLetter(input[i]) || input[i] == '_')
                {
                    var start = i;
                    while (i < input.Length && (char.IsLetterOrDigit(input[i]) || input[i] == '_' || input[i] == '.'))
                        i++;
                    var value = input.Substring(start, i - start);

                    // Check for keywords
                    if (value == "and" || value == "or" || value == "not" || value == "in")
                        tokens.Add(new Token(TokenType.Keyword, value));
                    else if (value == "true" || value == "false")
                        tokens.Add(new Token(TokenType.Boolean, value));
                    else
                        tokens.Add(new Token(TokenType.Identifier, value));
                    continue;
                }

                // String literals
                if (input[i] == '"' || input[i] == '\'')
                {
                    var quote = input[i];
                    var start = ++i;
                    while (i < input.Length && input[i] != quote)
                    {
                        if (input[i] == '\\' && i + 1 < input.Length) i++; // Skip escaped characters
                        i++;
                    }
                    if (i >= input.Length)
                        throw new InvalidOperationException($"Unterminated string literal starting at position {start - 1}");
                    tokens.Add(new Token(TokenType.String, input.Substring(start, i - start)));
                    i++; // Skip closing quote
                    continue;
                }

                // Two-character operators — avoid Substring allocation by comparing chars directly
                if (i + 1 < input.Length)
                {
                    var twoCharOp = MatchTwoCharOperator(input[i], input[i + 1]);
                    if (twoCharOp != null)
                    {
                        tokens.Add(new Token(TokenType.Operator, twoCharOp));
                        i += 2;
                        continue;
                    }
                }

                // Single-character operators and delimiters — avoid input[i].ToString() allocation
                var singleCharOp = MatchSingleChar(input[i]);
                if (singleCharOp != null)
                {
                    var type = input[i] == '(' || input[i] == ')' ? TokenType.Delimiter : TokenType.Operator;
                    tokens.Add(new Token(type, singleCharOp));
                    i++;
                    continue;
                }

                throw new InvalidOperationException($"Unexpected character in filter expression: '{input[i]}' at position {i}");
            }

            return tokens;
        }

        /// <summary>
        /// Match a two-character operator. Returns the cached string or null.
        /// Avoids Substring(i, 2) allocation on every iteration.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static string MatchTwoCharOperator(char c1, char c2)
        {
            return c1 switch
            {
                '=' when c2 == '=' => OpEqualEqual,
                '!' when c2 == '=' => OpBangEqual,
                '>' when c2 == '=' => OpGreaterEqual,
                '<' when c2 == '=' => OpLessEqual,
                '&' when c2 == '&' => OpAmpAmp,
                '|' when c2 == '|' => OpPipePipe,
                '*' when c2 == '*' => OpStarStar,
                _ => null
            };
        }

        /// <summary>
        /// Match a single-character operator or delimiter. Returns the cached string or null.
        /// Avoids char.ToString() allocation.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static string MatchSingleChar(char c)
        {
            return c switch
            {
                '+' => OpPlus,
                '-' => OpMinus,
                '*' => OpStar,
                '/' => OpSlash,
                '%' => OpPercent,
                '>' => OpGreater,
                '<' => OpLess,
                '!' => OpBang,
                '(' => OpOpenParen,
                ')' => OpCloseParen,
                _ => null
            };
        }
    }
}