// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Text;
using Garnet.server;

namespace Garnet.test
{
    /// <summary>
    /// Test helpers for the Redis-style filter pipeline.
    /// Compiles filter expressions and runs them against JSON attribute data.
    /// </summary>
    internal static class ExprTestHelpers
    {
        /// <summary>
        /// Compile and run a filter expression against JSON, returning the result as an ExprToken.
        /// This is useful for testing arithmetic/comparison results.
        /// </summary>
        internal static ExprToken EvaluateFilter(string expression, string json)
        {
            var filterBytes = Encoding.UTF8.GetBytes(expression);
            Span<ExprToken> instrBuf = stackalloc ExprToken[128];
            Span<ExprToken> tuplePoolBuf = stackalloc ExprToken[64];
            Span<ExprToken> tokensBuf = stackalloc ExprToken[128];
            Span<ExprToken> opsStackBuf = stackalloc ExprToken[128];
            var instrCount = ExprCompiler.TryCompile(filterBytes, instrBuf, tuplePoolBuf, tokensBuf, opsStackBuf, out var tupleCount, out var errpos);
            if (instrCount < 0)
                throw new InvalidOperationException($"Compilation failed at position {errpos}");

            var jsonBytes = Encoding.UTF8.GetBytes(json);

            Span<ExprToken> runtimePoolBuf = stackalloc ExprToken[64];
            var program = new ExprProgram
            {
                Instructions = instrBuf[..instrCount],
                Length = instrCount,
                TuplePool = tuplePoolBuf[..tupleCount],
                TuplePoolLength = tupleCount,
                RuntimePool = runtimePoolBuf,
                RuntimePoolLength = 0,
            };

            return RunAndReturnTop(ref program, filterBytes, jsonBytes);
        }

        /// <summary>
        /// Compile and run a filter expression against JSON, returning a boolean result.
        /// </summary>
        internal static bool EvaluateFilterTruthy(string expression, string json)
        {
            var filterBytes = Encoding.UTF8.GetBytes(expression);
            Span<ExprToken> instrBuf = stackalloc ExprToken[128];
            Span<ExprToken> tuplePoolBuf = stackalloc ExprToken[64];
            Span<ExprToken> tokensBuf = stackalloc ExprToken[128];
            Span<ExprToken> opsStackBuf = stackalloc ExprToken[128];
            var instrCount = ExprCompiler.TryCompile(filterBytes, instrBuf, tuplePoolBuf, tokensBuf, opsStackBuf, out var tupleCount, out var errpos);
            if (instrCount < 0)
                throw new InvalidOperationException($"Compilation failed at position {errpos}");

            var jsonBytes = Encoding.UTF8.GetBytes(json);

            Span<ExprToken> runtimePoolBuf = stackalloc ExprToken[64];
            var program = new ExprProgram
            {
                Instructions = instrBuf[..instrCount],
                Length = instrCount,
                TuplePool = tuplePoolBuf[..tupleCount],
                TuplePoolLength = tupleCount,
                RuntimePool = runtimePoolBuf,
                RuntimePoolLength = 0,
            };

            Span<(int, int)> selectorBuf = stackalloc (int, int)[32];
            var selectorCount = 0;
            for (var i = 0; i < instrCount; i++)
            {
                if (instrBuf[i].TokenType != ExprTokenType.Selector) continue;
                var s = instrBuf[i].Utf8Start;
                var l = instrBuf[i].Utf8Length;
                ReadOnlySpan<byte> span = filterBytes.AsSpan(s, l);
                var found = false;
                for (var j = 0; j < selectorCount; j++)
                    if (((ReadOnlySpan<byte>)filterBytes.AsSpan(selectorBuf[j].Item1, selectorBuf[j].Item2)).SequenceEqual(span)) { found = true; break; }
                if (!found) selectorBuf[selectorCount++] = (s, l);
            }
            var selectorRanges = selectorBuf[..selectorCount];

            Span<ExprToken> extractedFields = stackalloc ExprToken[selectorCount > 0 ? selectorCount : 1];
            AttributeExtractor.ExtractFields(jsonBytes, filterBytes, selectorRanges, extractedFields, ref program);
            Span<ExprToken> stackBuf = stackalloc ExprToken[16];
            var stack = new ExprStack(stackBuf);
            return ExprRunner.Run(ref program, jsonBytes, filterBytes, selectorRanges, extractedFields, ref stack);
        }

        /// <summary>
        /// Execute a compiled program and return the top-of-stack value (for testing).
        /// This is a test-only method that mirrors ExprRunner.Run but returns the raw result
        /// instead of a boolean, so tests can inspect numeric/string values.
        /// </summary>
        private static ExprToken RunAndReturnTop(ref ExprProgram program, byte[] filterBytes, byte[] jsonBytes)
        {
            ReadOnlySpan<byte> json = jsonBytes;
            ReadOnlySpan<byte> filter = filterBytes;
            var stack = new ExprToken[256];
            var stackLen = 0;

            for (var i = 0; i < program.Length; i++)
            {
                var inst = program.Instructions[i];

                if (inst.TokenType == ExprTokenType.Selector)
                {
                    var selectorName = filter.Slice(inst.Utf8Start, inst.Utf8Length);
                    var extracted = AttributeExtractor.ExtractField(json, selectorName);
                    if (extracted.IsNone)
                        return ExprToken.NewNull();

                    stack[stackLen++] = extracted;
                    continue;
                }

                if (inst.TokenType != ExprTokenType.Op)
                {
                    stack[stackLen++] = inst;
                    continue;
                }

                var arity = OpTable.GetArity(inst.OpCode);
                ExprToken b = stackLen > 0 ? stack[--stackLen] : default;
                ExprToken a = arity == 2 && stackLen > 0 ? stack[--stackLen] : default;

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
                        result.Num = AreEqual(a, b, program, filterBytes, json) ? 1 : 0;
                        break;
                    case OpCode.Neq:
                        result.Num = !AreEqual(a, b, program, filterBytes, json) ? 1 : 0;
                        break;
                    case OpCode.In:
                        result.Num = EvalIn(a, b, program, filterBytes, json) ? 1 : 0;
                        break;
                    case OpCode.And:
                        result.Num = ToBool(a, filterBytes, json) != 0 && ToBool(b, filterBytes, json) != 0 ? 1 : 0;
                        break;
                    case OpCode.Or:
                        result.Num = ToBool(a, filterBytes, json) != 0 || ToBool(b, filterBytes, json) != 0 ? 1 : 0;
                        break;
                }

                stack[stackLen++] = result;
            }

            return stackLen > 0 ? stack[stackLen - 1] : ExprToken.NewNull();
        }

        /// <summary>
        /// Resolve the UTF-8 bytes for a Str token. Filter-origin tokens reference
        /// filterBytes; extracted tokens reference json.
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
                // Try parsing UTF-8 bytes as a number
                if (double.TryParse(Encoding.UTF8.GetString(slice.ToArray()),
                    System.Globalization.NumberStyles.Float | System.Globalization.NumberStyles.AllowLeadingSign,
                    System.Globalization.CultureInfo.InvariantCulture, out var result))
                    return result;
                return 0;
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

        private static bool AreEqual(ExprToken a, ExprToken b, ExprProgram program,
            ReadOnlySpan<byte> filterBytes, ReadOnlySpan<byte> json)
        {
            if (a.IsNone || b.IsNone) return a.IsNone && b.IsNone;

            if (a.TokenType == ExprTokenType.Str && b.TokenType == ExprTokenType.Str)
            {
                var aSpan = GetStrSpan(a, filterBytes, json);
                var bSpan = GetStrSpan(b, filterBytes, json);
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

        private static bool EvalIn(ExprToken a, ExprToken b, ExprProgram program,
            ReadOnlySpan<byte> filterBytes, ReadOnlySpan<byte> json)
        {
            if (b.IsNone) return false;

            // Tuple membership: for Tuple tokens, Utf8Start = pool start, Utf8Length = element count
            if (b.TokenType == ExprTokenType.Tuple)
            {
                var poolStart = b.Utf8Start;
                var poolLen = b.Utf8Length;
                for (var i = 0; i < poolLen; i++)
                {
                    if (AreEqual(a, program.TuplePool[poolStart + i], program, filterBytes, json))
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
                return haystack.IndexOf(needle) >= 0;
            }

            return false;
        }

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
