// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Text;
using System.Collections.Generic;

namespace Garnet.server
{
    /// <summary>
    /// Shunting-Yard compiler that tokenizes and compiles a filter expression string
    /// into a flat postfix <see cref="ExprProgram"/>.
    ///
    /// All string and selector tokens are stored as (offset, length) byte-range references
    /// into the original filter expression bytes — zero string allocations.
    /// Tuple elements are stored in a flat pool on the program.
    /// </summary>
    internal static class ExprCompiler
    {
        private const int DefaultCapacity = 16;
        private const int MaxTupleElements = 64;

        /// <summary>
        /// Compile a filter expression (as UTF-8 bytes) into a flat postfix program.
        /// Returns null on syntax error; optionally reports the error position.
        /// </summary>
        public static ExprProgram TryCompile(ReadOnlySpan<byte> expr, out int errpos)
        {
            errpos = -1;
            if (expr.IsEmpty)
                return null;

            // Keep the original expression bytes — tokens will reference ranges within them
            var filterBytes = expr.ToArray();
            var exprLen = expr.Length;

            // Tuple pool: collects all tuple element tokens across all tuples
            var tuplePool = new List<ExprToken>(DefaultCapacity);

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
                    if (t.IsNone) { errpos = exprLen - remaining.Length; return null; }
                    tokens.Add(t);
                    continue;
                }

                // String literal — store (offset, length) into filter bytes
                if (remaining[0] == (byte)'"' || remaining[0] == (byte)'\'')
                {
                    var t = ParseString(exprLen, ref remaining);
                    if (t.IsNone) { errpos = exprLen - remaining.Length; return null; }
                    tokens.Add(t);
                    continue;
                }

                // Selector (field access starting with '.')
                if (remaining[0] == (byte)'.' && remaining.Length > 1 && IsSelectorChar(remaining[1]))
                {
                    var t = ParseSelector(exprLen, ref remaining);
                    tokens.Add(t);
                    continue;
                }

                // Tuple literal [1, "foo", 42]
                if (remaining[0] == (byte)'[')
                {
                    var t = ParseTuple(exprLen, tuplePool, ref remaining);
                    if (t.IsNone) { errpos = exprLen - remaining.Length; return null; }
                    tokens.Add(t);
                    continue;
                }

                // Operator or literal keyword (null, true, false, not, and, or, in)
                if (AttributeExtractor.IsLetter(remaining[0]) || IsOperatorSpecialChar(remaining[0]))
                {
                    var t = ParseOperatorOrLiteral(ref remaining);
                    if (t.IsNone) { errpos = exprLen - remaining.Length; return null; }
                    tokens.Add(t);
                    continue;
                }

                errpos = exprLen - remaining.Length;
                return null;
            }

            // Phase 2: Shunting-yard compilation to postfix
            var program = new List<ExprToken>(DefaultCapacity);
            var opsStack = new Stack<ExprToken>(DefaultCapacity);
            var stackItems = 0;

            for (var i = 0; i < tokens.Count; i++)
            {
                var token = tokens[i];

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

                if (token.TokenType == ExprTokenType.Op)
                {
                    if (!ProcessOperator(token, program, opsStack, ref stackItems, out errpos))
                        return null;
                    continue;
                }
            }

            while (opsStack.Count > 0)
            {
                var op = opsStack.Pop();
                if (op.OpCode == OpCode.OParen)
                {
                    errpos = 0;
                    return null;
                }
                var arity = OpTable.GetArity(op.OpCode);
                if (stackItems < arity) { errpos = 0; return null; }
                program.Add(op);
                stackItems = stackItems - arity + 1;
            }

            if (stackItems != 1) { errpos = 0; return null; }

            return new ExprProgram
            {
                Instructions = program.ToArray(),
                Length = program.Count,
                FilterBytes = filterBytes,
                TuplePool = tuplePool.Count > 0 ? tuplePool.ToArray() : [],
                TuplePoolLength = tuplePool.Count,
            };
        }

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
                while (true)
                {
                    if (opsStack.Count == 0) { errpos = 0; return false; }
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

            while (opsStack.Count > 0)
            {
                var topOp = opsStack.Peek();
                if (topOp.OpCode == OpCode.OParen) break;
                var topPrec = OpTable.GetPrecedence(topOp.OpCode);
                if (topPrec < curPrec) break;
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

        /// <summary>
        /// Parse a string literal. Returns a Str token with (offset, length) into the
        /// original filter expression bytes — zero allocation.
        /// </summary>
        private static ExprToken ParseString(int exprLen, ref ReadOnlySpan<byte> s)
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
                    var contentLen = body.Length - s.Length;
                    // Absolute offset = exprLen - body.Length (position of first char after opening quote)
                    var absOffset = exprLen - body.Length;
                    s = s[1..]; // Skip closing quote
                    return ExprToken.NewFilterStr(absOffset, contentLen, hasEscape);
                }
                s = s[1..];
            }
            return default; // Unterminated string
        }

        /// <summary>
        /// Parse a selector (.fieldName). Returns a Selector token with (offset, length)
        /// into the original filter expression bytes — zero allocation.
        /// </summary>
        private static ExprToken ParseSelector(int exprLen, ref ReadOnlySpan<byte> s)
        {
            s = s[1..]; // Skip the leading dot
            var start = s;
            while (!s.IsEmpty && IsSelectorChar(s[0])) s = s[1..];
            var nameLen = start.Length - s.Length;
            var absOffset = exprLen - start.Length;
            return ExprToken.NewSelector(absOffset, nameLen);
        }

        /// <summary>
        /// Parse a tuple literal [1, "foo", 42]. Elements are stored in the shared
        /// <paramref name="tuplePool"/>, and the token stores (poolStartIndex, count).
        /// </summary>
        private static ExprToken ParseTuple(int exprLen, List<ExprToken> tuplePool, ref ReadOnlySpan<byte> s)
        {
            s = s[1..]; // Skip '['
            s = AttributeExtractor.TrimWhiteSpace(s);

            // Handle empty tuple []
            if (!s.IsEmpty && s[0] == (byte)']')
            {
                s = s[1..];
                return ExprToken.NewTuple(0, 0);
            }

            var poolStart = tuplePool.Count;
            var count = 0;

            while (true)
            {
                s = AttributeExtractor.TrimWhiteSpace(s);
                if (s.IsEmpty) return default;
                if (count >= MaxTupleElements) return default;

                ExprToken ele;
                if (AttributeExtractor.IsDigit(s[0]) || s[0] == (byte)'-')
                {
                    ele = ParseNumber(ref s);
                }
                else if (s[0] == (byte)'"' || s[0] == (byte)'\'')
                {
                    ele = ParseString(exprLen, ref s);
                }
                else
                {
                    return default;
                }
                if (ele.IsNone) return default;

                tuplePool.Add(ele);
                count++;

                s = AttributeExtractor.TrimWhiteSpace(s);
                if (s.IsEmpty) return default;

                if (s[0] == (byte)']') { s = s[1..]; break; }
                if (s[0] != (byte)',') return default;
                s = s[1..];
            }

            return ExprToken.NewTuple(poolStart, count);
        }

        private static ExprToken ParseOperatorOrLiteral(ref ReadOnlySpan<byte> s)
        {
            var start = s;

            while (!s.IsEmpty && (AttributeExtractor.IsLetter(s[0]) || IsOperatorSpecialChar(s[0])))
                s = s[1..];

            var consumed = start[..(start.Length - s.Length)];
            if (consumed.IsEmpty) return default;

            if (consumed.Length == 4 && consumed.SequenceEqual("null"u8))
                return ExprToken.NewNull();

            if (consumed.Length == 4 && consumed.SequenceEqual("true"u8))
                return ExprToken.NewNum(1);

            if (consumed.Length == 5 && consumed.SequenceEqual("false"u8))
                return ExprToken.NewNum(0);

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
