// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Globalization;
using System.Runtime.CompilerServices;

namespace Garnet.server.Vector.Filter
{
    /// <summary>
    /// Shunting-yard compiler that tokenizes and compiles a filter expression string
    /// into a flat postfix <see cref="ExprProgram"/>.
    ///
    /// Single-pass tokenize-and-compile approach modeled after Redis expr.c.
    ///
    /// The compiled program is a flat array of <see cref="ExprToken"/> instructions
    /// (values + operators in postfix order) that can be executed by <see cref="ExprRunner"/>.
    ///
    /// Safety limits:
    /// - Maximum 1024 tokens per expression (prevents unbounded allocation).
    /// - Maximum 256 instructions in the compiled program.
    /// </summary>
    internal static class ExprCompiler
    {
        private const int MaxTokens = 1024;
        private const int MaxProgram = 256;
        private const string SelectorSpecialChars = "_-";
        private const string OperatorSpecialChars = "+-*%/!()<>=|&";

        /// <summary>
        /// Compile a filter expression string into a flat postfix program.
        /// Returns null on syntax error; optionally reports the error position.
        /// </summary>
        public static ExprProgram TryCompile(string expr, out int errpos)
        {
            errpos = -1;
            if (string.IsNullOrEmpty(expr))
                return null;

            // Phase 1: Tokenize into a flat list
            var tokens = new ExprToken[MaxTokens];
            var numTokens = 0;

            var p = 0;
            while (p < expr.Length)
            {
                SkipSpaces(expr, ref p);
                if (p >= expr.Length)
                    break;

                if (numTokens >= MaxTokens)
                {
                    errpos = p;
                    return null;
                }

                // Determine if '-' should be a negative number sign or a subtraction operator
                var minusIsNumber = false;
                if (expr[p] == '-' && p + 1 < expr.Length && (char.IsDigit(expr[p + 1]) || expr[p + 1] == '.'))
                {
                    if (numTokens == 0)
                    {
                        minusIsNumber = true;
                    }
                    else
                    {
                        var prev = tokens[numTokens - 1];
                        if (prev.TokenType == ExprTokenType.Op && prev.OpCode != OpCode.CParen)
                            minusIsNumber = true;
                    }
                }

                // Number
                if (char.IsDigit(expr[p]) || (minusIsNumber && expr[p] == '-'))
                {
                    var t = ParseNumber(expr, ref p);
                    if (t == null) { errpos = p; return null; }
                    tokens[numTokens++] = t;
                    continue;
                }

                // String literal
                if (expr[p] == '"' || expr[p] == '\'')
                {
                    var t = ParseString(expr, ref p);
                    if (t == null) { errpos = p; return null; }
                    tokens[numTokens++] = t;
                    continue;
                }

                // Selector (field access starting with '.')
                if (expr[p] == '.' && p + 1 < expr.Length && IsSelectorChar(expr[p + 1]))
                {
                    var t = ParseSelector(expr, ref p);
                    tokens[numTokens++] = t;
                    continue;
                }

                // Tuple literal [1, "foo", 42]
                if (expr[p] == '[')
                {
                    var t = ParseTuple(expr, ref p);
                    if (t == null) { errpos = p; return null; }
                    tokens[numTokens++] = t;
                    continue;
                }

                // Operator or literal keyword (null, true, false, not, and, or, in)
                if (char.IsLetter(expr[p]) || OperatorSpecialChars.IndexOf(expr[p]) >= 0)
                {
                    var t = ParseOperatorOrLiteral(expr, ref p);
                    if (t == null) { errpos = p; return null; }
                    tokens[numTokens++] = t;
                    continue;
                }

                errpos = p;
                return null;
            }

            // Phase 2: Shunting-yard compilation to postfix
            var program = new ExprToken[MaxProgram];
            var programLen = 0;
            var opsStack = new ExprToken[MaxTokens];
            var opsLen = 0;
            var stackItems = 0; // track what would be on the values stack at runtime

            for (var i = 0; i < numTokens; i++)
            {
                var token = tokens[i];

                // Values go directly to program
                if (token.TokenType == ExprTokenType.Num ||
                    token.TokenType == ExprTokenType.Str ||
                    token.TokenType == ExprTokenType.Tuple ||
                    token.TokenType == ExprTokenType.Selector ||
                    token.TokenType == ExprTokenType.Null)
                {
                    if (programLen >= MaxProgram) { errpos = 0; return null; }
                    program[programLen++] = token;
                    stackItems++;
                    continue;
                }

                // Operators
                if (token.TokenType == ExprTokenType.Op)
                {
                    if (!ProcessOperator(token, program, ref programLen, opsStack, ref opsLen, ref stackItems, out errpos))
                        return null;
                    continue;
                }
            }

            // Flush remaining operators from the stack
            while (opsLen > 0)
            {
                var op = opsStack[--opsLen];
                if (op.OpCode == OpCode.OParen)
                {
                    errpos = 0;
                    return null; // Unmatched '('
                }

                var arity = OpTable.GetArity(op.OpCode);
                if (stackItems < arity) { errpos = 0; return null; }
                if (programLen >= MaxProgram) { errpos = 0; return null; }
                program[programLen++] = op;
                stackItems = stackItems - arity + 1;
            }

            // After compilation, exactly one value should remain on the stack
            if (stackItems != 1) { errpos = 0; return null; }

            return new ExprProgram { Instructions = program, Length = programLen };
        }

        /// <summary>
        /// Process an operator during shunting-yard compilation.
        /// Handles parentheses, precedence, and right-associativity of **.
        /// </summary>
        private static bool ProcessOperator(
            ExprToken op,
            ExprToken[] program, ref int programLen,
            ExprToken[] opsStack, ref int opsLen,
            ref int stackItems,
            out int errpos)
        {
            errpos = -1;

            if (op.OpCode == OpCode.OParen)
            {
                if (opsLen >= opsStack.Length) { errpos = 0; return false; }
                opsStack[opsLen++] = op;
                return true;
            }

            if (op.OpCode == OpCode.CParen)
            {
                // Pop operators until matching '('
                while (true)
                {
                    if (opsLen == 0) { errpos = 0; return false; } // Unmatched ')'
                    var topOp = opsStack[--opsLen];
                    if (topOp.OpCode == OpCode.OParen)
                        return true;

                    var arity = OpTable.GetArity(topOp.OpCode);
                    if (stackItems < arity) { errpos = 0; return false; }
                    if (programLen >= MaxProgram) { errpos = 0; return false; }
                    program[programLen++] = topOp;
                    stackItems = stackItems - arity + 1;
                }
            }

            var curPrec = OpTable.GetPrecedence(op.OpCode);

            // Pop operators with higher or equal precedence
            while (opsLen > 0)
            {
                var topOp = opsStack[opsLen - 1];
                if (topOp.OpCode == OpCode.OParen) break;

                var topPrec = OpTable.GetPrecedence(topOp.OpCode);
                if (topPrec < curPrec) break;

                // Right-associative: ** only pops if strictly higher
                if (op.OpCode == OpCode.Pow && topPrec <= curPrec) break;

                opsLen--;
                var arity = OpTable.GetArity(topOp.OpCode);
                if (stackItems < arity) { errpos = 0; return false; }
                if (programLen >= MaxProgram) { errpos = 0; return false; }
                program[programLen++] = topOp;
                stackItems = stackItems - arity + 1;
            }

            if (opsLen >= opsStack.Length) { errpos = 0; return false; }
            opsStack[opsLen++] = op;
            return true;
        }

        // ======================== Tokenization helpers ========================

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void SkipSpaces(string s, ref int p)
        {
            while (p < s.Length && char.IsWhiteSpace(s[p])) p++;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool IsSelectorChar(char c)
        {
            return char.IsLetterOrDigit(c) || SelectorSpecialChars.IndexOf(c) >= 0;
        }

        private static ExprToken ParseNumber(string s, ref int p)
        {
            var start = p;
            if (p < s.Length && s[p] == '-') p++;

            while (p < s.Length && (char.IsDigit(s[p]) || s[p] == '.' || s[p] == 'e' || s[p] == 'E'))
                p++;

            var numStr = s.Substring(start, p - start);
            if (!double.TryParse(numStr, NumberStyles.Float | NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out var value))
            {
                p = start;
                return null;
            }
            return ExprToken.NewNum(value);
        }

        private static ExprToken ParseString(string s, ref int p)
        {
            var quote = s[p];
            p++; // Skip opening quote
            var start = p;
            var hasEscape = false;

            while (p < s.Length)
            {
                if (s[p] == '\\' && p + 1 < s.Length)
                {
                    hasEscape = true;
                    p += 2; // Skip escaped char
                    continue;
                }
                if (s[p] == quote)
                {
                    string value;
                    if (!hasEscape)
                    {
                        value = s.Substring(start, p - start);
                    }
                    else
                    {
                        // Process escape sequences (matching Redis fastjson.c behavior)
                        var chars = new char[p - start];
                        var len = 0;
                        for (var i = start; i < p; i++)
                        {
                            if (s[i] == '\\' && i + 1 < p)
                            {
                                i++;
                                chars[len++] = s[i] switch
                                {
                                    'n' => '\n',
                                    'r' => '\r',
                                    't' => '\t',
                                    '\\' => '\\',
                                    '"' => '"',
                                    '\'' => '\'',
                                    _ => s[i], // Unknown escape — copy verbatim
                                };
                            }
                            else
                            {
                                chars[len++] = s[i];
                            }
                        }
                        value = new string(chars, 0, len);
                    }
                    p++; // Skip closing quote
                    return ExprToken.NewStr(value);
                }
                p++;
            }
            return null; // Unterminated string
        }

        private static ExprToken ParseSelector(string s, ref int p)
        {
            p++; // Skip the leading dot
            var start = p;
            while (p < s.Length && IsSelectorChar(s[p])) p++;
            var name = s.Substring(start, p - start);
            return ExprToken.NewSelector(name);
        }

        private static ExprToken ParseTuple(string s, ref int p)
        {
            p++; // Skip '['
            var elements = new ExprToken[64]; // max 64 elements
            var count = 0;

            SkipSpaces(s, ref p);

            // Handle empty tuple []
            if (p < s.Length && s[p] == ']')
            {
                p++;
                return ExprToken.NewTuple([], 0);
            }

            while (true)
            {
                SkipSpaces(s, ref p);
                if (p >= s.Length) return null;
                if (count >= elements.Length) return null;

                // Parse element: number or string
                ExprToken ele;
                if (char.IsDigit(s[p]) || s[p] == '-')
                {
                    ele = ParseNumber(s, ref p);
                }
                else if (s[p] == '"' || s[p] == '\'')
                {
                    ele = ParseString(s, ref p);
                }
                else
                {
                    return null;
                }
                if (ele == null) return null;

                elements[count++] = ele;

                SkipSpaces(s, ref p);
                if (p >= s.Length) return null;

                if (s[p] == ']') { p++; break; }
                if (s[p] != ',') return null;
                p++; // Skip comma
            }

            var result = new ExprToken[count];
            Array.Copy(elements, result, count);
            return ExprToken.NewTuple(result, count);
        }

        private static ExprToken ParseOperatorOrLiteral(string s, ref int p)
        {
            var start = p;

            // Consume alphabetic or operator-special characters
            while (p < s.Length && (char.IsLetter(s[p]) || OperatorSpecialChars.IndexOf(s[p]) >= 0))
                p++;

            var matchLen = p - start;
            if (matchLen == 0) return null;

            // Check for literals
            if (matchLen == 4 && string.Compare(s, start, "null", 0, 4, StringComparison.Ordinal) == 0)
                return ExprToken.NewNull();

            if (matchLen == 4 && string.Compare(s, start, "true", 0, 4, StringComparison.Ordinal) == 0)
                return ExprToken.NewNum(1);

            if (matchLen == 5 && string.Compare(s, start, "false", 0, 5, StringComparison.Ordinal) == 0)
                return ExprToken.NewNum(0);

            // Find best matching operator (longest match)
            OpCode bestCode = default;
            var bestLen = 0;
            TryMatchOp(s, start, matchLen, "||", OpCode.Or, ref bestCode, ref bestLen);
            TryMatchOp(s, start, matchLen, "or", OpCode.Or, ref bestCode, ref bestLen);
            TryMatchOp(s, start, matchLen, "&&", OpCode.And, ref bestCode, ref bestLen);
            TryMatchOp(s, start, matchLen, "and", OpCode.And, ref bestCode, ref bestLen);
            TryMatchOp(s, start, matchLen, "**", OpCode.Pow, ref bestCode, ref bestLen);
            TryMatchOp(s, start, matchLen, ">=", OpCode.Gte, ref bestCode, ref bestLen);
            TryMatchOp(s, start, matchLen, "<=", OpCode.Lte, ref bestCode, ref bestLen);
            TryMatchOp(s, start, matchLen, "==", OpCode.Eq, ref bestCode, ref bestLen);
            TryMatchOp(s, start, matchLen, "!=", OpCode.Neq, ref bestCode, ref bestLen);
            TryMatchOp(s, start, matchLen, "not", OpCode.Not, ref bestCode, ref bestLen);
            TryMatchOp(s, start, matchLen, "in", OpCode.In, ref bestCode, ref bestLen);
            TryMatchOp(s, start, matchLen, "(", OpCode.OParen, ref bestCode, ref bestLen);
            TryMatchOp(s, start, matchLen, ")", OpCode.CParen, ref bestCode, ref bestLen);
            TryMatchOp(s, start, matchLen, "+", OpCode.Add, ref bestCode, ref bestLen);
            TryMatchOp(s, start, matchLen, "-", OpCode.Sub, ref bestCode, ref bestLen);
            TryMatchOp(s, start, matchLen, "*", OpCode.Mul, ref bestCode, ref bestLen);
            TryMatchOp(s, start, matchLen, "/", OpCode.Div, ref bestCode, ref bestLen);
            TryMatchOp(s, start, matchLen, "%", OpCode.Mod, ref bestCode, ref bestLen);
            TryMatchOp(s, start, matchLen, ">", OpCode.Gt, ref bestCode, ref bestLen);
            TryMatchOp(s, start, matchLen, "<", OpCode.Lt, ref bestCode, ref bestLen);
            TryMatchOp(s, start, matchLen, "!", OpCode.Not, ref bestCode, ref bestLen);

            if (bestLen == 0)
            {
                p = start;
                return null;
            }

            // Rewind p to consume only the matched operator length
            p = start + bestLen;
            return ExprToken.NewOp(bestCode);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void TryMatchOp(string s, int start, int matchLen, string opName, OpCode opCode, ref OpCode bestCode, ref int bestLen)
        {
            var opLen = opName.Length;
            if (opLen > matchLen) return;
            if (string.Compare(s, start, opName, 0, opLen, StringComparison.Ordinal) != 0) return;
            if (opLen > bestLen)
            {
                bestCode = opCode;
                bestLen = opLen;
            }
        }
    }
}
