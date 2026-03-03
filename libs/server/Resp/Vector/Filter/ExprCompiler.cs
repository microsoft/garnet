// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Text;
using System.Collections.Generic;
using System.Text;

namespace Garnet.server.Vector.Filter
{
    /// <summary>
    /// Shunting-Yard compiler that tokenizes and compiles a filter expression string
    /// into a flat postfix <see cref="ExprProgram"/>.
    ///
    /// Single-pass tokenize-and-compile approach modeled after Redis expr.c.
    ///
    /// The compiled program is a flat array of <see cref="ExprToken"/> instructions
    /// (values + operators in postfix order) that can be executed by <see cref="ExprRunner"/>.
    ///
    /// Example:
    /// Input expression:
    ///   .price &lt; 100 and .category == "books"
    /// Compiled postfix order:
    ///   .price 100 &lt; .category "books" == and
    ///
    /// </summary>
    internal static class ExprCompiler
    {
        private const int DefaultCapacity = 16;

        /// <summary>
        /// Compile a filter expression (as UTF-8 bytes) into a flat postfix program.
        /// Returns null on syntax error; optionally reports the error position.
        /// </summary>
        public static ExprProgram TryCompile(ReadOnlySpan<byte> expr, out int errpos)
        {
            errpos = -1;
            if (expr.IsEmpty)
                return null;

            // Phase 1: Tokenize into a flat list
            var tokens = new List<ExprToken>(DefaultCapacity);

            var p = 0;
            while (p < expr.Length)
            {
                AttributeExtractor.SkipWhiteSpace(expr, ref p);
                if (p >= expr.Length)
                    break;

                // Determine if '-' should be a negative number sign or a subtraction operator
                var minusIsNumber = false;
                if (expr[p] == (byte)'-' && p + 1 < expr.Length && (AttributeExtractor.IsDigit(expr[p + 1]) || expr[p + 1] == (byte)'.'))
                {
                    if (tokens.Count == 0)
                    {
                        minusIsNumber = true;
                    }
                    else
                    {
                        var prev = tokens[tokens.Count - 1];
                        if (prev.TokenType == ExprTokenType.Op && prev.OpCode != OpCode.CParen)
                            minusIsNumber = true;
                    }
                }

                // Number
                if (AttributeExtractor.IsDigit(expr[p]) || (minusIsNumber && expr[p] == (byte)'-'))
                {
                    var t = ParseNumber(expr, ref p);
                    if (t.IsNone) { errpos = p; return null; }
                    tokens.Add(t);
                    continue;
                }

                // String literal
                if (expr[p] == (byte)'"' || expr[p] == (byte)'\'')
                {
                    var t = ParseString(expr, ref p);
                    if (t.IsNone) { errpos = p; return null; }
                    tokens.Add(t);
                    continue;
                }

                // Selector (field access starting with '.')
                if (expr[p] == (byte)'.' && p + 1 < expr.Length && IsSelectorChar(expr[p + 1]))
                {
                    var t = ParseSelector(expr, ref p);
                    tokens.Add(t);
                    continue;
                }

                // Tuple literal [1, "foo", 42]
                if (expr[p] == (byte)'[')
                {
                    var t = ParseTuple(expr, ref p);
                    if (t.IsNone) { errpos = p; return null; }
                    tokens.Add(t);
                    continue;
                }

                // Operator or literal keyword (null, true, false, not, and, or, in)
                if (AttributeExtractor.IsLetter(expr[p]) || IsOperatorSpecialChar(expr[p]))
                {
                    var t = ParseOperatorOrLiteral(expr, ref p);
                    if (t.IsNone) { errpos = p; return null; }
                    tokens.Add(t);
                    continue;
                }

                errpos = p;
                return null;
            }

            // Phase 2: Shunting-yard compilation to postfix
            var program = new List<ExprToken>(DefaultCapacity);
            var opsStack = new Stack<ExprToken>(DefaultCapacity);
            var stackItems = 0; // track what would be on the values stack at runtime

            for (var i = 0; i < tokens.Count; i++)
            {
                var token = tokens[i];

                // Values go directly to program
                if (token.TokenType == ExprTokenType.Num ||
                    token.TokenType == ExprTokenType.Str ||
                    token.TokenType == ExprTokenType.Tuple ||
                    token.TokenType == ExprTokenType.Selector ||
                    token.TokenType == ExprTokenType.Null)
                {
                    program.Add(token);
                    stackItems++;
                    continue;
                }

                // Operators
                if (token.TokenType == ExprTokenType.Op)
                {
                    if (!ProcessOperator(token, program, opsStack, ref stackItems, out errpos))
                        return null;
                    continue;
                }
            }

            // Flush remaining operators from the stack
            while (opsStack.Count > 0)
            {
                var op = opsStack.Pop();
                if (op.OpCode == OpCode.OParen)
                {
                    errpos = 0;
                    return null; // Unmatched '('
                }

                var arity = OpTable.GetArity(op.OpCode);
                if (stackItems < arity) { errpos = 0; return null; }
                program.Add(op);
                stackItems = stackItems - arity + 1;
            }

            // After compilation, exactly one value should remain on the stack
            if (stackItems != 1) { errpos = 0; return null; }

            return new ExprProgram { Instructions = program.ToArray(), Length = program.Count };
        }

        /// <summary>
        /// Process an operator during shunting-yard compilation.
        /// Handles parentheses, precedence, and right-associativity of **.
        /// </summary>
        private static bool ProcessOperator(
            ExprToken op,
            List<ExprToken> program,
            Stack<ExprToken> opsStack,
            ref int stackItems,
            out int errpos)
        {
            errpos = -1;

            if (op.OpCode == OpCode.OParen)
            {
                opsStack.Push(op);
                return true;
            }

            if (op.OpCode == OpCode.CParen)
            {
                // Pop operators until matching '('
                while (true)
                {
                    if (opsStack.Count == 0) { errpos = 0; return false; } // Unmatched ')'
                    var topOp = opsStack.Pop();
                    if (topOp.OpCode == OpCode.OParen)
                        return true;

                    var arity = OpTable.GetArity(topOp.OpCode);
                    if (stackItems < arity) { errpos = 0; return false; }
                    program.Add(topOp);
                    stackItems = stackItems - arity + 1;
                }
            }

            var curPrec = OpTable.GetPrecedence(op.OpCode);

            // Pop operators with higher or equal precedence
            while (opsStack.Count > 0)
            {
                var topOp = opsStack.Peek();
                if (topOp.OpCode == OpCode.OParen) break;

                var topPrec = OpTable.GetPrecedence(topOp.OpCode);
                if (topPrec < curPrec) break;

                // Right-associative: ** only pops if strictly higher
                if (op.OpCode == OpCode.Pow && topPrec <= curPrec) break;

                opsStack.Pop();
                var arity = OpTable.GetArity(topOp.OpCode);
                if (stackItems < arity) { errpos = 0; return false; }
                program.Add(topOp);
                stackItems = stackItems - arity + 1;
            }

            opsStack.Push(op);
            return true;
        }

        // ======================== Tokenization helpers ========================
        // Shared helpers (IsDigit, IsLetter, IsLetterOrDigit, IsWhiteSpace, SkipWhiteSpace)
        // live in AttributeExtractor and are reused here.

        private static bool IsOperatorSpecialChar(byte b)
        {
            return b == (byte)'+' || b == (byte)'-' || b == (byte)'*' || b == (byte)'%' ||
                   b == (byte)'/' || b == (byte)'!' || b == (byte)'(' || b == (byte)')' ||
                   b == (byte)'<' || b == (byte)'>' || b == (byte)'=' || b == (byte)'|' ||
                   b == (byte)'&';
        }

        private static bool IsSelectorChar(byte c)
        {
            return AttributeExtractor.IsLetterOrDigit(c) || c == (byte)'_' || c == (byte)'-';
        }

        private static ExprToken ParseNumber(ReadOnlySpan<byte> s, ref int p)
        {
            var start = p;
            if (p < s.Length && s[p] == (byte)'-') p++;

            while (p < s.Length && (AttributeExtractor.IsDigit(s[p]) || s[p] == (byte)'.' || s[p] == (byte)'e' || s[p] == (byte)'E'))
                p++;

            var numSpan = s.Slice(start, p - start);
            if (!Utf8Parser.TryParse(numSpan, out double value, out var bytesConsumed) || bytesConsumed != numSpan.Length)
            {
                p = start;
                return default;
            }
            return ExprToken.NewNum(value);
        }

        private static ExprToken ParseString(ReadOnlySpan<byte> s, ref int p)
        {
            var quote = s[p];
            p++; // Skip opening quote
            var start = p;
            var hasEscape = false;

            while (p < s.Length)
            {
                if (s[p] == (byte)'\\' && p + 1 < s.Length)
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
                        value = Encoding.UTF8.GetString(s.Slice(start, p - start));
                    }
                    else
                    {
                        // Process escape sequences (matching Redis fastjson.c behavior)
                        var bytes = new byte[p - start];
                        var len = 0;
                        for (var i = start; i < p; i++)
                        {
                            if (s[i] == (byte)'\\' && i + 1 < p)
                            {
                                i++;
                                bytes[len++] = s[i] switch
                                {
                                    (byte)'n' => (byte)'\n',
                                    (byte)'r' => (byte)'\r',
                                    (byte)'t' => (byte)'\t',
                                    (byte)'\\' => (byte)'\\',
                                    (byte)'"' => (byte)'"',
                                    (byte)'\'' => (byte)'\'',
                                    _ => s[i], // Unknown escape — copy verbatim
                                };
                            }
                            else
                            {
                                bytes[len++] = s[i];
                            }
                        }
                        value = Encoding.UTF8.GetString(bytes, 0, len);
                    }
                    p++; // Skip closing quote
                    return ExprToken.NewStr(value);
                }
                p++;
            }
            return default; // Unterminated string
        }

        private static ExprToken ParseSelector(ReadOnlySpan<byte> s, ref int p)
        {
            p++; // Skip the leading dot
            var start = p;
            while (p < s.Length && IsSelectorChar(s[p])) p++;
            var name = Encoding.UTF8.GetString(s.Slice(start, p - start));
            return ExprToken.NewSelector(name);
        }

        private static ExprToken ParseTuple(ReadOnlySpan<byte> s, ref int p)
        {
            p++; // Skip '['
            var elements = new ExprToken[64]; // max 64 elements
            var count = 0;

            AttributeExtractor.SkipWhiteSpace(s, ref p);

            // Handle empty tuple []
            if (p < s.Length && s[p] == (byte)']')
            {
                p++;
                return ExprToken.NewTuple([], 0);
            }

            while (true)
            {
                AttributeExtractor.SkipWhiteSpace(s, ref p);
                if (p >= s.Length) return default;
                if (count >= elements.Length) return default;

                // Parse element: number or string
                ExprToken ele;
                if (AttributeExtractor.IsDigit(s[p]) || s[p] == (byte)'-')
                {
                    ele = ParseNumber(s, ref p);
                }
                else if (s[p] == (byte)'"' || s[p] == (byte)'\'')
                {
                    ele = ParseString(s, ref p);
                }
                else
                {
                    return default;
                }
                if (ele.IsNone) return default;

                elements[count++] = ele;

                AttributeExtractor.SkipWhiteSpace(s, ref p);
                if (p >= s.Length) return default;

                if (s[p] == (byte)']') { p++; break; }
                if (s[p] != (byte)',') return default;
                p++; // Skip comma
            }

            var result = new ExprToken[count];
            Array.Copy(elements, result, count);
            return ExprToken.NewTuple(result, count);
        }

        private static ExprToken ParseOperatorOrLiteral(ReadOnlySpan<byte> s, ref int p)
        {
            var start = p;

            // Consume alphabetic or operator-special characters
            while (p < s.Length && (AttributeExtractor.IsLetter(s[p]) || IsOperatorSpecialChar(s[p])))
                p++;

            var matchLen = p - start;
            if (matchLen == 0) return default;

            // Check for literals
            if (matchLen == 4 && s.Slice(start, 4).SequenceEqual("null"u8))
                return ExprToken.NewNull();

            if (matchLen == 4 && s.Slice(start, 4).SequenceEqual("true"u8))
                return ExprToken.NewNum(1);

            if (matchLen == 5 && s.Slice(start, 5).SequenceEqual("false"u8))
                return ExprToken.NewNum(0);

            // Find best matching operator (longest match)
            OpCode bestCode = default;
            var bestLen = 0;
            var consumed = s.Slice(start, matchLen);
            TryMatchOp(consumed, "||"u8, OpCode.Or, ref bestCode, ref bestLen);
            TryMatchOp(consumed, "or"u8, OpCode.Or, ref bestCode, ref bestLen);
            TryMatchOp(consumed, "&&"u8, OpCode.And, ref bestCode, ref bestLen);
            TryMatchOp(consumed, "and"u8, OpCode.And, ref bestCode, ref bestLen);
            TryMatchOp(consumed, "**"u8, OpCode.Pow, ref bestCode, ref bestLen);
            TryMatchOp(consumed, ">="u8, OpCode.Gte, ref bestCode, ref bestLen);
            TryMatchOp(consumed, "<="u8, OpCode.Lte, ref bestCode, ref bestLen);
            TryMatchOp(consumed, "=="u8, OpCode.Eq, ref bestCode, ref bestLen);
            TryMatchOp(consumed, "!="u8, OpCode.Neq, ref bestCode, ref bestLen);
            TryMatchOp(consumed, "not"u8, OpCode.Not, ref bestCode, ref bestLen);
            TryMatchOp(consumed, "in"u8, OpCode.In, ref bestCode, ref bestLen);
            TryMatchOp(consumed, "("u8, OpCode.OParen, ref bestCode, ref bestLen);
            TryMatchOp(consumed, ")"u8, OpCode.CParen, ref bestCode, ref bestLen);
            TryMatchOp(consumed, "+"u8, OpCode.Add, ref bestCode, ref bestLen);
            TryMatchOp(consumed, "-"u8, OpCode.Sub, ref bestCode, ref bestLen);
            TryMatchOp(consumed, "*"u8, OpCode.Mul, ref bestCode, ref bestLen);
            TryMatchOp(consumed, "/"u8, OpCode.Div, ref bestCode, ref bestLen);
            TryMatchOp(consumed, "%"u8, OpCode.Mod, ref bestCode, ref bestLen);
            TryMatchOp(consumed, ">"u8, OpCode.Gt, ref bestCode, ref bestLen);
            TryMatchOp(consumed, "<"u8, OpCode.Lt, ref bestCode, ref bestLen);
            TryMatchOp(consumed, "!"u8, OpCode.Not, ref bestCode, ref bestLen);

            if (bestLen == 0)
            {
                p = start;
                return default;
            }

            // Rewind p to consume only the matched operator length
            p = start + bestLen;
            return ExprToken.NewOp(bestCode);
        }

        private static void TryMatchOp(ReadOnlySpan<byte> consumed, ReadOnlySpan<byte> opName, OpCode opCode, ref OpCode bestCode, ref int bestLen)
        {
            var opLen = opName.Length;
            if (opLen > consumed.Length) return;
            if (!consumed.Slice(0, opLen).SequenceEqual(opName)) return;
            if (opLen > bestLen)
            {
                bestCode = opCode;
                bestLen = opLen;
            }
        }
    }
}