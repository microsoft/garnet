// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Text;

namespace Garnet.server
{
    /// <summary>
    /// Lightweight stack over a caller-supplied <see cref="Span{ExprToken}"/> buffer.
    /// Zero heap allocation — designed for stackalloc use.
    /// </summary>
    internal ref struct ExprStack
    {
        private readonly Span<ExprToken> _buffer;
        private int _count;

        public ExprStack(Span<ExprToken> buffer) { _buffer = buffer; _count = 0; }
        public int Count => _count;
        public bool IsFull => _count >= _buffer.Length;
        public void Push(ExprToken t) => _buffer[_count++] = t;
        public bool TryPush(ExprToken t) { if (_count >= _buffer.Length) return false; _buffer[_count++] = t; return true; }
        public ExprToken Pop() => _buffer[--_count];
        public ExprToken Peek() => _buffer[_count - 1];
        public void Clear() => _count = 0;
    }

    /// <summary>
    /// Stack-based VM that executes a compiled <see cref="ExprProgram"/> against
    /// raw JSON attribute bytes.
    ///
    /// All string comparisons work on raw UTF-8 byte spans — no string allocations.
    /// Tokens reference byte ranges in two source buffers:
    /// - <b>filterBytes</b>: compile-time string literals and selector names
    /// - <b>json</b>: runtime-extracted string values
    /// </summary>
    internal static class ExprRunner
    {
        /// <summary>
        /// Execute the compiled program using pre-extracted field values.
        /// Selectors are resolved from <paramref name="extractedFields"/> by matching byte ranges.
        /// Uses <see cref="ExprStack"/> (backed by stackalloc) — zero heap allocation.
        /// </summary>
        public static bool Run(
            ref ExprProgram program,
            ReadOnlySpan<byte> json,
            ReadOnlySpan<byte> filterBytes,
            ReadOnlySpan<(int Start, int Length)> selectorRanges,
            Span<ExprToken> extractedFields,
            ref ExprStack stack)
        {
            stack.Clear();

            for (var i = 0; i < program.Length; i++)
            {
                var inst = program.Instructions[i];

                if (inst.TokenType == ExprTokenType.Selector)
                {
                    var selectorName = filterBytes.Slice(inst.Utf8Start, inst.Utf8Length);
                    var found = false;
                    for (var j = 0; j < selectorRanges.Length; j++)
                    {
                        if (selectorName.SequenceEqual(filterBytes.Slice(selectorRanges[j].Start, selectorRanges[j].Length)))
                        {
                            if (extractedFields[j].IsNone)
                            {
                                stack.Clear();
                                return false;
                            }
                            if (!stack.TryPush(extractedFields[j]))
                            {
                                stack.Clear();
                                return false;
                            }
                            found = true;
                            break;
                        }
                    }
                    if (!found) { stack.Clear(); return false; }
                    continue;
                }

                if (!ExecuteInstruction(inst, ref program, filterBytes, json, ref stack))
                    return false;
            }

            var returnValue = false;
            if (stack.Count > 0)
                returnValue = ToBool(stack.Peek(), filterBytes, json) != 0;

            stack.Clear();
            return returnValue;
        }

        private static bool ExecuteInstruction(
            ExprToken inst,
            ref ExprProgram program,
            ReadOnlySpan<byte> filterBytes,
            ReadOnlySpan<byte> json,
            ref ExprStack stack)
        {
            if (inst.TokenType != ExprTokenType.Op)
            {
                if (!stack.TryPush(inst)) { stack.Clear(); return false; }
                return true;
            }

            var arity = OpTable.GetArity(inst.OpCode);
            if (stack.Count < arity) { stack.Clear(); return false; }

            ExprToken b = stack.Count > 0 ? stack.Pop() : default;
            ExprToken a = arity == 2 && stack.Count > 0 ? stack.Pop() : default;

            var result = ExprToken.NewNum(0);

            switch (inst.OpCode)
            {
                case OpCode.Not:
                    result.Num = ToBool(b, filterBytes, json) == 0 ? 1 : 0;
                    break;
                case OpCode.Pow:
                    result.Num = Math.Pow(ToNum(a, filterBytes, json), ToNum(b, filterBytes, json));
                    break;
                case OpCode.Mul:
                    result.Num = ToNum(a, filterBytes, json) * ToNum(b, filterBytes, json);
                    break;
                case OpCode.Div:
                    result.Num = ToNum(a, filterBytes, json) / ToNum(b, filterBytes, json);
                    break;
                case OpCode.Mod:
                    result.Num = ToNum(a, filterBytes, json) % ToNum(b, filterBytes, json);
                    break;
                case OpCode.Add:
                    result.Num = ToNum(a, filterBytes, json) + ToNum(b, filterBytes, json);
                    break;
                case OpCode.Sub:
                    result.Num = ToNum(a, filterBytes, json) - ToNum(b, filterBytes, json);
                    break;
                case OpCode.Gt:
                    result.Num = ToNum(a, filterBytes, json) > ToNum(b, filterBytes, json) ? 1 : 0;
                    break;
                case OpCode.Gte:
                    result.Num = ToNum(a, filterBytes, json) >= ToNum(b, filterBytes, json) ? 1 : 0;
                    break;
                case OpCode.Lt:
                    result.Num = ToNum(a, filterBytes, json) < ToNum(b, filterBytes, json) ? 1 : 0;
                    break;
                case OpCode.Lte:
                    result.Num = ToNum(a, filterBytes, json) <= ToNum(b, filterBytes, json) ? 1 : 0;
                    break;
                case OpCode.Eq:
                    result.Num = AreEqual(a, b, ref program, filterBytes, json) ? 1 : 0;
                    break;
                case OpCode.Neq:
                    result.Num = !AreEqual(a, b, ref program, filterBytes, json) ? 1 : 0;
                    break;
                case OpCode.In:
                    result.Num = EvalIn(a, b, ref program, filterBytes, json) ? 1 : 0;
                    break;
                case OpCode.And:
                    result.Num = ToBool(a, filterBytes, json) != 0 && ToBool(b, filterBytes, json) != 0 ? 1 : 0;
                    break;
                case OpCode.Or:
                    result.Num = ToBool(a, filterBytes, json) != 0 || ToBool(b, filterBytes, json) != 0 ? 1 : 0;
                    break;
            }

            if (!stack.TryPush(result)) { stack.Clear(); return false; }
            return true;
        }

        // ======================== Type conversion helpers ========================

        /// <summary>
        /// Resolve the UTF-8 bytes for a Str token. Tokens from the compiler
        /// (<see cref="ExprToken.IsFilterOrigin"/> = true) reference filterBytes;
        /// tokens from the extractor reference json.
        /// </summary>
        private static ReadOnlySpan<byte> GetStrSpan(ExprToken t, ReadOnlySpan<byte> filterBytes, ReadOnlySpan<byte> json)
        {
            return t.IsFilterOrigin
                ? filterBytes.Slice(t.Utf8Start, t.Utf8Length)
                : json.Slice(t.Utf8Start, t.Utf8Length);
        }

        private static double ToNum(ExprToken t, ReadOnlySpan<byte> filterBytes, ReadOnlySpan<byte> json)
        {
            if (t.IsNone) return 0;
            if (t.TokenType == ExprTokenType.Num) return t.Num;
            if (t.TokenType == ExprTokenType.Str)
            {
                var slice = GetStrSpan(t, filterBytes, json);
                return Utf8Parser.TryParse(slice, out double result, out var consumed) && consumed == slice.Length ? result : 0;
            }
            return 0;
        }

        private static double ToBool(ExprToken t, ReadOnlySpan<byte> filterBytes, ReadOnlySpan<byte> json)
        {
            if (t.IsNone) return 0;
            if (t.TokenType == ExprTokenType.Num) return t.Num != 0 ? 1 : 0;
            if (t.TokenType == ExprTokenType.Str) return t.Utf8Length == 0 ? 0 : 1;
            if (t.TokenType == ExprTokenType.Null) return 0;
            return 1;
        }

        private static bool AreEqual(ExprToken a, ExprToken b, ref ExprProgram program,
            ReadOnlySpan<byte> filterBytes, ReadOnlySpan<byte> json)
        {
            if (a.IsNone || b.IsNone) return a.IsNone && b.IsNone;

            if (a.TokenType == ExprTokenType.Str && b.TokenType == ExprTokenType.Str)
            {
                var aSpan = GetStrSpan(a, filterBytes, json);
                var bSpan = GetStrSpan(b, filterBytes, json);
                // If either has escape sequences, need to handle them
                if (!a.HasEscape && !b.HasEscape)
                    return aSpan.SequenceEqual(bSpan);
                return UnescapedEquals(aSpan, a.HasEscape, bSpan, b.HasEscape);
            }

            if (a.TokenType == ExprTokenType.Num && b.TokenType == ExprTokenType.Num)
                return a.Num == b.Num;

            if (a.TokenType == ExprTokenType.Null || b.TokenType == ExprTokenType.Null)
                return a.TokenType == b.TokenType;

            return ToNum(a, filterBytes, json) == ToNum(b, filterBytes, json);
        }

        private static bool EvalIn(ExprToken a, ExprToken b, ref ExprProgram program,
            ReadOnlySpan<byte> filterBytes, ReadOnlySpan<byte> json)
        {
            if (b.IsNone) return false;

            // Tuple membership
            if (b.TokenType == ExprTokenType.Tuple)
            {
                var poolStart = b.Utf8Start;
                var poolLen = b.Utf8Length;
                var pool = b.IsRuntimeTuple ? program.RuntimePool : program.TuplePool;
                for (var i = 0; i < poolLen; i++)
                {
                    if (AreEqual(a, pool[poolStart + i], ref program, filterBytes, json))
                        return true;
                }
                return false;
            }

            // String substring check
            if (!a.IsNone && a.TokenType == ExprTokenType.Str && b.TokenType == ExprTokenType.Str)
            {
                var needle = GetStrSpan(a, filterBytes, json);
                var haystack = GetStrSpan(b, filterBytes, json);
                if (needle.Length == 0) return true;
                if (needle.Length > haystack.Length) return false;
                // For escaped strings, this is approximate — exact unescape-and-search
                // would be needed for full correctness with escape sequences in IN.
                return haystack.IndexOf(needle) >= 0;
            }

            return false;
        }

        // ======================== Escape-aware comparison ========================

        /// <summary>
        /// Compare two UTF-8 byte spans for equality, handling JSON escape sequences
        /// in either or both. Unescapes on the fly without allocating.
        /// </summary>
        private static bool UnescapedEquals(ReadOnlySpan<byte> a, bool aEscaped, ReadOnlySpan<byte> b, bool bEscaped)
        {
            var ai = 0;
            var bi = 0;
            while (ai < a.Length && bi < b.Length)
            {
                byte ac, bc;
                if (aEscaped && ai < a.Length - 1 && a[ai] == (byte)'\\')
                {
                    ai++;
                    ac = UnescapeByte(a[ai]);
                }
                else
                {
                    ac = a[ai];
                }

                if (bEscaped && bi < b.Length - 1 && b[bi] == (byte)'\\')
                {
                    bi++;
                    bc = UnescapeByte(b[bi]);
                }
                else
                {
                    bc = b[bi];
                }

                if (ac != bc) return false;
                ai++;
                bi++;
            }

            // Both must be exhausted
            return ai == a.Length && bi == b.Length;
        }

        private static byte UnescapeByte(byte b) => b switch
        {
            (byte)'n' => (byte)'\n',
            (byte)'r' => (byte)'\r',
            (byte)'t' => (byte)'\t',
            (byte)'\\' => (byte)'\\',
            (byte)'"' => (byte)'"',
            (byte)'\'' => (byte)'\'',
            (byte)'/' => (byte)'/',
            _ => b,
        };
    }
}