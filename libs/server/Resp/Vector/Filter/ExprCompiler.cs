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
            var remaining = expr;

            while (!remaining.IsEmpty)
            {
                remaining = AttributeExtractor.TrimWhiteSpace(remaining);
                if (remaining.IsEmpty)
                    break;

                // Determine if '-' should be a negative number sign or a subtraction operator
                var minusIsNumber = false;
                if (remaining[0] == (byte)'-' && remaining.Length > 1 && (AttributeExtractor.IsDigit(remaining[1]) || remaining[1] == (byte)'.'))
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
                if (AttributeExtractor.IsDigit(remaining[0]) || (minusIsNumber && remaining[0] == (byte)'-'))
                {
                    var t = ParseNumber(ref remaining);
                    if (t.IsNone) { errpos = expr.Length - remaining.Length; return null; }
                    tokens.Add(t);
                    continue;
                }

                // String literal
                if (remaining[0] == (byte)'"' || remaining[0] == (byte)'\'')
                {
                    var t = ParseString(ref remaining);
                    if (t.IsNone) { errpos = expr.Length - remaining.Length; return null; }
                    tokens.Add(t);
                    continue;
                }

                // Selector (field access starting with '.')
                if (remaining[0] == (byte)'.' && remaining.Length > 1 && IsSelectorChar(remaining[1]))
                {
                    var t = ParseSelector(ref remaining);
                    tokens.Add(t);
                    continue;
                }

                // Tuple literal [1, "foo", 42]
                if (remaining[0] == (byte)'[')
                {
                    var t = ParseTuple(ref remaining);
                    if (t.IsNone) { errpos = expr.Length - remaining.Length; return null; }
                    tokens.Add(t);
                    continue;
                }

                // Operator or literal keyword (null, true, false, not, and, or, in)
                if (AttributeExtractor.IsLetter(remaining[0]) || IsOperatorSpecialChar(remaining[0]))
                {
                    var t = ParseOperatorOrLiteral(ref remaining);
                    if (t.IsNone) { errpos = expr.Length - remaining.Length; return null; }
                    tokens.Add(t);
                    continue;
                }

                errpos = expr.Length - remaining.Length;
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
        // Shared helpers (IsDigit, IsLetter, IsLetterOrDigit, IsWhiteSpace, TrimWhiteSpace)
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

        private static ExprToken ParseNumber(ref ReadOnlySpan<byte> s)
        {
            var original = s;
            if (s[0] == (byte)'-') s = s[1..];

            while (!s.IsEmpty && (AttributeExtractor.IsDigit(s[0]) || s[0] == (byte)'.' || s[0] == (byte)'e' || s[0] == (byte)'E'))
                s = s[1..];

            var numSpan = original[..(original.Length - s.Length)];
            if (!Utf8Parser.TryParse(numSpan, out double value, out var bytesConsumed) || bytesConsumed != numSpan.Length)
            {
                s = original;
                return default;
            }
            return ExprToken.NewNum(value);
        }

        private static ExprToken ParseString(ref ReadOnlySpan<byte> s)
        {
            var quote = s[0];
            s = s[1..]; // Skip opening quote
            var body = s;
            var hasEscape = false;

            while (!s.IsEmpty)
            {
                if (s[0] == (byte)'\\' && s.Length > 1)
                {
                    hasEscape = true;
                    s = s[2..]; // Skip escaped char
                    continue;
                }
                if (s[0] == quote)
                {
                    var content = body[..(body.Length - s.Length)];
                    string value;
                    if (!hasEscape)
                    {
                        value = Encoding.UTF8.GetString(content);
                    }
                    else
                    {
                        // Process escape sequences (matching Redis fastjson.c behavior)
                        var bytes = new byte[content.Length];
                        var len = 0;
                        for (var i = 0; i < content.Length; i++)
                        {
                            if (content[i] == (byte)'\\' && i + 1 < content.Length)
                            {
                                i++;
                                bytes[len++] = content[i] switch
                                {
                                    (byte)'n' => (byte)'\n',
                                    (byte)'r' => (byte)'\r',
                                    (byte)'t' => (byte)'\t',
                                    (byte)'\\' => (byte)'\\',
                                    (byte)'"' => (byte)'"',
                                    (byte)'\'' => (byte)'\'',
                                    _ => content[i], // Unknown escape — copy verbatim
                                };
                            }
                            else
                            {
                                bytes[len++] = content[i];
                            }
                        }
                        value = Encoding.UTF8.GetString(bytes, 0, len);
                    }
                    s = s[1..]; // Skip closing quote
                    return ExprToken.NewStr(value);
                }
                s = s[1..];
            }
            return default; // Unterminated string
        }

        private static ExprToken ParseSelector(ref ReadOnlySpan<byte> s)
        {
            s = s[1..]; // Skip the leading dot
            var start = s;
            while (!s.IsEmpty && IsSelectorChar(s[0])) s = s[1..];
            var name = Encoding.UTF8.GetString(start[..(start.Length - s.Length)]);
            return ExprToken.NewSelector(name);
        }

        private static ExprToken ParseTuple(ref ReadOnlySpan<byte> s)
        {
            s = s[1..]; // Skip '['
            var elements = new ExprToken[64]; // max 64 elements
            var count = 0;

            s = AttributeExtractor.TrimWhiteSpace(s);

            // Handle empty tuple []
            if (!s.IsEmpty && s[0] == (byte)']')
            {
                s = s[1..];
                return ExprToken.NewTuple([], 0);
            }

            while (true)
            {
                s = AttributeExtractor.TrimWhiteSpace(s);
                if (s.IsEmpty) return default;
                if (count >= elements.Length) return default;

                // Parse element: number or string
                ExprToken ele;
                if (AttributeExtractor.IsDigit(s[0]) || s[0] == (byte)'-')
                {
                    ele = ParseNumber(ref s);
                }
                else if (s[0] == (byte)'"' || s[0] == (byte)'\'')
                {
                    ele = ParseString(ref s);
                }
                else
                {
                    return default;
                }
                if (ele.IsNone) return default;

                elements[count++] = ele;

                s = AttributeExtractor.TrimWhiteSpace(s);
                if (s.IsEmpty) return default;

                if (s[0] == (byte)']') { s = s[1..]; break; }
                if (s[0] != (byte)',') return default;
                s = s[1..]; // Skip comma
            }

            var result = new ExprToken[count];
            Array.Copy(elements, result, count);
            return ExprToken.NewTuple(result, count);
        }

        private static ExprToken ParseOperatorOrLiteral(ref ReadOnlySpan<byte> s)
        {
            var start = s;

            // Consume alphabetic or operator-special characters
            while (!s.IsEmpty && (AttributeExtractor.IsLetter(s[0]) || IsOperatorSpecialChar(s[0])))
                s = s[1..];

            var consumed = start[..(start.Length - s.Length)];
            if (consumed.IsEmpty) return default;

            // Check for literals
            if (consumed.Length == 4 && consumed.SequenceEqual("null"u8))
                return ExprToken.NewNull();

            if (consumed.Length == 4 && consumed.SequenceEqual("true"u8))
                return ExprToken.NewNum(1);

            if (consumed.Length == 5 && consumed.SequenceEqual("false"u8))
                return ExprToken.NewNum(0);

            // Find best matching operator (longest match)
            OpCode bestCode = default;
            var bestLen = 0;
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
                s = start;
                return default;
            }

            // Rewind — only consume the matched operator length
            s = start[bestLen..];
            return ExprToken.NewOp(bestCode);
        }

        private static void TryMatchOp(ReadOnlySpan<byte> consumed, ReadOnlySpan<byte> opName, OpCode opCode, ref OpCode bestCode, ref int bestLen)
        {
            if (opName.Length > consumed.Length) return;
            if (!consumed[..opName.Length].SequenceEqual(opName)) return;
            if (opName.Length > bestLen)
            {
                bestCode = opCode;
                bestLen = opName.Length;
            }
        }
    }
}