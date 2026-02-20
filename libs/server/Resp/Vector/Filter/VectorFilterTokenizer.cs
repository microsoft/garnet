// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;

namespace Garnet.server.Vector.Filter
{
    /// <summary>
    /// Token types for vector filter expressions.
    /// </summary>
    internal enum TokenType { Number, String, Boolean, Identifier, Operator, Keyword, Delimiter }

    /// <summary>
    /// Represents a token in a filter expression.
    /// </summary>
    internal class Token
    {
        public TokenType Type { get; set; }
        public string Value { get; set; }
    }

    /// <summary>
    /// Tokenizer for vector filter expressions.
    /// Converts filter strings into tokens for parsing.
    /// </summary>
    internal static class VectorFilterTokenizer
    {
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
                        || (tokens[^1].Type == TokenType.Delimiter && tokens[^1].Value == "("));

                if (char.IsDigit(input[i]) || isNegativeNumber)
                {
                    var start = i;
                    if (input[i] == '-') i++;
                    while (i < input.Length && (char.IsDigit(input[i]) || input[i] == '.'))
                        i++;
                    tokens.Add(new Token { Type = TokenType.Number, Value = input.Substring(start, i - start) });
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
                        tokens.Add(new Token { Type = TokenType.Keyword, Value = value });
                    else if (value == "true" || value == "false")
                        tokens.Add(new Token { Type = TokenType.Boolean, Value = value });
                    else
                        tokens.Add(new Token { Type = TokenType.Identifier, Value = value });
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
                    tokens.Add(new Token { Type = TokenType.String, Value = input.Substring(start, i - start) });
                    i++; // Skip closing quote
                    continue;
                }

                // Two-character operators
                if (i + 1 < input.Length)
                {
                    var twoChar = input.Substring(i, 2);
                    if (twoChar == "==" || twoChar == "!=" || twoChar == ">=" || twoChar == "<=" ||
                        twoChar == "&&" || twoChar == "||" || twoChar == "**")
                    {
                        tokens.Add(new Token { Type = TokenType.Operator, Value = twoChar });
                        i += 2;
                        continue;
                    }
                }

                // Single-character operators and delimiters
                if ("+-*/%><!()".Contains(input[i]))
                {
                    tokens.Add(new Token { Type = input[i] == '(' || input[i] == ')' ? TokenType.Delimiter : TokenType.Operator, Value = input[i].ToString() });
                    i++;
                    continue;
                }

                throw new InvalidOperationException($"Unexpected character in filter expression: '{input[i]}' at position {i}");
            }

            return tokens;
        }
    }
}