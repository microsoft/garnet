// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Text;
using System.Text;

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
                if (expr[p] == (byte)'-' && p + 1 < expr.Length && (IsDigit(expr[p + 1]) || expr[p + 1] == (byte)'.'))
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
                if (IsDigit(expr[p]) || (minusIsNumber && expr[p] == (byte)'-'))
                {
                    var t = ParseNumber(expr, ref p);
                    if (t == null) { errpos = p; return null; }
                    tokens[numTokens++] = t;
                    continue;
                }

                // String literal
                if (expr[p] == (byte)'"' || expr[p] == (byte)'\'')
                {
                    var t = ParseString(expr, ref p);
                    if (t == null) { errpos = p; return null; }
                    tokens[numTokens++] = t;
                    continue;
                }

                // Selector (field access starting with '.')
                if (expr[p] == (byte)'.' && p + 1 < expr.Length && IsSelectorChar(expr[p + 1]))
                {
                    var t = ParseSelector(expr, ref p);
                    tokens[numTokens++] = t;
                    continue;
                }

                // Tuple literal [1, "foo", 42]
                if (expr[p] == (byte)'[')
                {
                    var t = ParseTuple(expr, ref p);
                    if (t == null) { errpos = p; return null; }
                    tokens[numTokens++] = t;
                    continue;
                }

                // Operator or literal keyword (null, true, false, not, and, or, in)
                if (IsLetter(expr[p]) || IsOperatorSpecialChar(expr[p]))
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

        private static bool IsDigit(byte b) => b >= (byte)'0' && b <= (byte)'9';

        private static bool IsLetter(byte b) => (b >= (byte)'a' && b <= (byte)'z') || (b >= (byte)'A' && b <= (byte)'Z');

        private static bool IsLetterOrDigit(byte b) => IsLetter(b) || IsDigit(b);

        private static bool IsWhiteSpace(byte b) => b == (byte)' ' || b == (byte)'\t' || b == (byte)'\n' || b == (byte)'\r';

        private static bool IsOperatorSpecialChar(byte b)
        {
            return b == (byte)'+' || b == (byte)'-' || b == (byte)'*' || b == (byte)'%' ||
                   b == (byte)'/' || b == (byte)'!' || b == (byte)'(' || b == (byte)')' ||
                   b == (byte)'<' || b == (byte)'>' || b == (byte)'=' || b == (byte)'|' ||
                   b == (byte)'&';
        }

        private static void SkipSpaces(ReadOnlySpan<byte> s, ref int p)
        {
            while (p < s.Length && IsWhiteSpace(s[p])) p++;
        }

        private static bool IsSelectorChar(byte c)
        {
            return IsLetterOrDigit(c) || c == (byte)'_' || c == (byte)'-';
        }

        private static ExprToken ParseNumber(ReadOnlySpan<byte> s, ref int p)
        {
            var start = p;
            if (p < s.Length && s[p] == (byte)'-') p++;

            while (p < s.Length && (IsDigit(s[p]) || s[p] == (byte)'.' || s[p] == (byte)'e' || s[p] == (byte)'E'))
                p++;

            var numSpan = s.Slice(start, p - start);
            if (!Utf8Parser.TryParse(numSpan, out double value, out var bytesConsumed) || bytesConsumed != numSpan.Length)
            {
                p = start;
                return null;
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
            return null; // Unterminated string
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

            SkipSpaces(s, ref p);

            // Handle empty tuple []
            if (p < s.Length && s[p] == (byte)']')
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
                if (IsDigit(s[p]) || s[p] == (byte)'-')
                {
                    ele = ParseNumber(s, ref p);
                }
                else if (s[p] == (byte)'"' || s[p] == (byte)'\'')
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

                if (s[p] == (byte)']') { p++; break; }
                if (s[p] != (byte)',') return null;
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
            while (p < s.Length && (IsLetter(s[p]) || IsOperatorSpecialChar(s[p])))
                p++;

            var matchLen = p - start;
            if (matchLen == 0) return null;

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
                return null;
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