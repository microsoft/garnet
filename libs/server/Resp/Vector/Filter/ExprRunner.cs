// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Text;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
namespace Garnet.server.Vector.Filter
{
    /// <summary>
    /// Stack-based VM that executes a compiled <see cref="ExprProgram"/> against
    /// raw JSON attribute bytes.
    ///
    /// Modeled after Redis expr.c exprRun() — walks the flat postfix program,
    /// pushes values, and pops operands for operators. Selectors trigger
    /// on-demand JSON field extraction via <see cref="AttributeExtractor"/>.
    ///
    /// Key design properties (matching Redis):
    /// - No DOM allocation: JSON fields are extracted directly from the raw bytes.
    /// - Compile once, run many: the program is reused across all candidate elements.
    /// - Exact numeric equality (no epsilon) to match Redis behavior.
    /// - Substring support for the IN operator when both sides are strings.
    /// - null is a first-class token type.
    /// </summary>
    internal static class ExprRunner
    {
        private const int DefaultStackCapacity = 16;

        /// <summary>
        /// Create a reusable evaluation stack with default capacity (16).
        /// The caller owns the stack and can pass it to <see cref="Run"/> across multiple calls.
        /// The stack is cleared at the start of each Run call, so the caller does not need to clear it.
        /// </summary>
        public static Stack<ExprToken> CreateStack() => new Stack<ExprToken>(DefaultStackCapacity);

        /// <summary>
        /// Execute the compiled program against JSON attribute data.
        /// Returns true if the expression evaluates to a truthy value, false otherwise.
        /// Returns false if the JSON is malformed or a selector cannot be resolved.
        /// </summary>
        /// <param name="program">The compiled postfix program.</param>
        /// <param name="json">Raw JSON attribute bytes to evaluate against.</param>
        /// <param name="stack">A reusable evaluation stack obtained from <see cref="CreateStack"/>.</param>
        public static bool Run(ExprProgram program, ReadOnlySpan<byte> json, Stack<ExprToken> stack)
        {
            stack.Clear();

            for (var i = 0; i < program.Length; i++)
            {
                var inst = program.Instructions[i];

                // Selectors — extract field from JSON
                if (inst.TokenType == ExprTokenType.Selector)
                {
                    var extracted = AttributeExtractor.ExtractField(json, inst.Str);
                    if (extracted.IsNone)
                    {
                        stack.Clear();
                        return false; // Selector not found → expression is false (matches Redis)
                    }

                    stack.Push(extracted);
                    continue;
                }

                // Non-operator values — push directly
                if (inst.TokenType != ExprTokenType.Op)
                {
                    stack.Push(inst);
                    continue;
                }

                // Operators — pop operands, compute, push result
                var arity = OpTable.GetArity(inst.OpCode);
                if (stack.Count < arity)
                {
                    stack.Clear();
                    return false;
                }

                ExprToken b = stack.Count > 0 ? stack.Pop() : default;
                ExprToken a = arity == 2 && stack.Count > 0 ? stack.Pop() : default;

                var result = ExprToken.NewNum(0);

                switch (inst.OpCode)
                {
                    case OpCode.Not:
                        result.Num = ToBool(b) == 0 ? 1 : 0;
                        break;
                    case OpCode.Pow:
                        result.Num = Math.Pow(ToNum(a, json), ToNum(b, json));
                        break;
                    case OpCode.Mul:
                        result.Num = ToNum(a, json) * ToNum(b, json);
                        break;
                    case OpCode.Div:
                        result.Num = ToNum(a, json) / ToNum(b, json);
                        break;
                    case OpCode.Mod:
                        result.Num = ToNum(a, json) % ToNum(b, json);
                        break;
                    case OpCode.Add:
                        result.Num = ToNum(a, json) + ToNum(b, json);
                        break;
                    case OpCode.Sub:
                        result.Num = ToNum(a, json) - ToNum(b, json);
                        break;
                    case OpCode.Gt:
                        result.Num = ToNum(a, json) > ToNum(b, json) ? 1 : 0;
                        break;
                    case OpCode.Gte:
                        result.Num = ToNum(a, json) >= ToNum(b, json) ? 1 : 0;
                        break;
                    case OpCode.Lt:
                        result.Num = ToNum(a, json) < ToNum(b, json) ? 1 : 0;
                        break;
                    case OpCode.Lte:
                        result.Num = ToNum(a, json) <= ToNum(b, json) ? 1 : 0;
                        break;
                    case OpCode.Eq:
                        result.Num = AreEqual(a, b, json) ? 1 : 0;
                        break;
                    case OpCode.Neq:
                        result.Num = !AreEqual(a, b, json) ? 1 : 0;
                        break;
                    case OpCode.In:
                        result.Num = EvalIn(a, b, json) ? 1 : 0;
                        break;
                    case OpCode.And:
                        result.Num = ToBool(a) != 0 && ToBool(b) != 0 ? 1 : 0;
                        break;
                    case OpCode.Or:
                        result.Num = ToBool(a) != 0 || ToBool(b) != 0 ? 1 : 0;
                        break;
                }

                stack.Push(result);
            }

            var returnValue = false;
            if (stack.Count > 0)
                returnValue = ToBool(stack.Peek()) != 0;

            // Clear to release string references for GC
            stack.Clear();
            return returnValue;
        }

        // ======================== Type conversion helpers ========================

        /// <summary>
        /// Convert a token to its numeric value.
        /// Strings are parsed as numbers; unparseable strings return 0.
        /// Matches Redis exprTokenToNum().
        /// </summary>
        private static double ToNum(ExprToken t, ReadOnlySpan<byte> json)
        {
            if (t.IsNone) return 0;
            if (t.TokenType == ExprTokenType.Num) return t.Num;
            if (t.TokenType == ExprTokenType.Str)
            {
                if (t.IsJsonRef)
                {
                    var slice = json.Slice(t.Utf8Start, t.Utf8Length);
                    return Utf8Parser.TryParse(slice, out double result, out var consumed) && consumed == slice.Length ? result : 0;
                }

                if (t.Str != null)
                {
                    return double.TryParse(t.Str, NumberStyles.Float | NumberStyles.AllowLeadingSign,
                        CultureInfo.InvariantCulture, out var result) ? result : 0;
                }
            }
            return 0;
        }

        /// <summary>
        /// Convert a token to boolean (0 or 1).
        /// Matches Redis exprTokenToBool(): null=0, num!=0=1, empty string=0, else=1.
        /// </summary>
        private static double ToBool(ExprToken t)
        {
            if (t.IsNone) return 0;
            if (t.TokenType == ExprTokenType.Num) return t.Num != 0 ? 1 : 0;
            if (t.TokenType == ExprTokenType.Str)
            {
                if (t.IsJsonRef) return t.Utf8Length == 0 ? 0 : 1;
                return (t.Str == null || t.Str.Length == 0) ? 0 : 1;
            }
            if (t.TokenType == ExprTokenType.Null) return 0;
            return 1; // Non-empty strings, tuples, etc. are truthy
        }

        /// <summary>
        /// Compare two tokens for equality.
        /// Matches Redis exprTokensEqual():
        /// - Both strings → exact string comparison (handles JSON refs)
        /// - Both numbers → exact numeric equality (no epsilon)
        /// - One/both null → equal only if both null
        /// - Mixed types → coerce to numbers and compare
        /// </summary>
        private static bool AreEqual(ExprToken a, ExprToken b, ReadOnlySpan<byte> json)
        {
            if (a.IsNone || b.IsNone) return a.IsNone && b.IsNone;

            // Both strings — handle 4 combinations of string/JsonRef
            if (a.TokenType == ExprTokenType.Str && b.TokenType == ExprTokenType.Str)
            {
                if (!a.IsJsonRef && !b.IsJsonRef)
                    return string.Equals(a.Str, b.Str, StringComparison.Ordinal);

                if (a.IsJsonRef && b.IsJsonRef)
                    return json.Slice(a.Utf8Start, a.Utf8Length).SequenceEqual(json.Slice(b.Utf8Start, b.Utf8Length));

                // One is a compiled string, one is a JSON ref
                var str = a.IsJsonRef ? b.Str : a.Str;
                var jsonRef = a.IsJsonRef ? a : b;
                return Utf8Equals(str, json.Slice(jsonRef.Utf8Start, jsonRef.Utf8Length));
            }

            // Both numbers
            if (a.TokenType == ExprTokenType.Num && b.TokenType == ExprTokenType.Num)
                return a.Num == b.Num; // Exact comparison, matching Redis

            // One/both null
            if (a.TokenType == ExprTokenType.Null || b.TokenType == ExprTokenType.Null)
                return a.TokenType == b.TokenType;

            // Mixed types — coerce to number
            return ToNum(a, json) == ToNum(b, json);
        }

        /// <summary>
        /// Evaluate the IN operator.
        /// Matches Redis expr.c behavior:
        /// 1. If b is a Tuple, check membership (element-wise AreEqual)
        /// 2. If both a and b are strings, check substring containment
        /// 3. Otherwise, false
        /// </summary>
        private static bool EvalIn(ExprToken a, ExprToken b, ReadOnlySpan<byte> json)
        {
            if (b.IsNone) return false;

            // Tuple membership (works for both expression tuples [1,2,3] and JSON array tuples)
            if (b.TokenType == ExprTokenType.Tuple)
            {
                for (var i = 0; i < b.TupleLength; i++)
                {
                    if (AreEqual(a, b.TupleElements[i], json))
                        return true;
                }
                return false;
            }

            // String substring check (matching Redis exprTokensStringIn)
            if (!a.IsNone && a.TokenType == ExprTokenType.Str && b.TokenType == ExprTokenType.Str)
            {
                // Both compiled strings
                if (!a.IsJsonRef && !b.IsJsonRef)
                {
                    if (a.Str == null || b.Str == null) return false;
                    if (a.Str.Length > b.Str.Length) return false;
                    return b.Str.IndexOf(a.Str, StringComparison.Ordinal) >= 0;
                }

                // Needle is compiled string, haystack is JSON ref (most common filter case)
                if (!a.IsJsonRef && b.IsJsonRef)
                {
                    if (a.Str == null) return false;
                    return Utf8Contains(json.Slice(b.Utf8Start, b.Utf8Length), a.Str);
                }

                // Needle is JSON ref, haystack is compiled string
                if (a.IsJsonRef && !b.IsJsonRef)
                {
                    if (b.Str == null) return false;
                    return Utf8ContainsReverse(b.Str, json.Slice(a.Utf8Start, a.Utf8Length));
                }

                // Both JSON refs
                var needleSlice = json.Slice(a.Utf8Start, a.Utf8Length);
                var haystackSlice = json.Slice(b.Utf8Start, b.Utf8Length);
                return haystackSlice.IndexOf(needleSlice) >= 0;
            }

            return false;
        }

        // ======================== UTF-8 byte comparison helpers ========================

        /// <summary>
        /// Compare a .NET string to raw UTF-8 bytes for equality without allocating.
        /// Uses ASCII fast path; falls back to encoding for non-ASCII.
        /// </summary>
        private static bool Utf8Equals(string str, ReadOnlySpan<byte> utf8)
        {
            // ASCII fast path: for single-byte chars, string length == byte length
            if (str.Length == utf8.Length)
            {
                for (var i = 0; i < utf8.Length; i++)
                {
                    if (str[i] > 127) goto slowPath;
                    if (utf8[i] != (byte)str[i]) return false;
                }
                return true;
            }

        slowPath:
            // Slow path for multi-byte UTF-8 characters (rare in filter expressions)
            var maxBytes = Encoding.UTF8.GetMaxByteCount(str.Length);
            Span<byte> buf = maxBytes <= 512 ? stackalloc byte[maxBytes] : new byte[maxBytes];
            var written = Encoding.UTF8.GetBytes(str.AsSpan(), buf);
            return utf8.SequenceEqual(buf[..written]);
        }

        /// <summary>
        /// Check if a UTF-8 byte span contains a .NET string as a substring.
        /// ASCII fast path; falls back to encoding for non-ASCII.
        /// </summary>
        private static bool Utf8Contains(ReadOnlySpan<byte> haystack, string needle)
        {
            if (needle.Length == 0) return true;
            if (needle.Length > haystack.Length) return false;

            // ASCII fast path
            for (var i = 0; i <= haystack.Length - needle.Length; i++)
            {
                if (haystack[i] == (byte)needle[0])
                {
                    var match = true;
                    for (var j = 1; j < needle.Length; j++)
                    {
                        if (needle[j] > 127) goto slowPath;
                        if (haystack[i + j] != (byte)needle[j]) { match = false; break; }
                    }
                    if (match) return true;
                }
            }
            return false;

        slowPath:
            var haystackStr = Encoding.UTF8.GetString(haystack);
            return haystackStr.IndexOf(needle, StringComparison.Ordinal) >= 0;
        }

        /// <summary>
        /// Check if a .NET string contains a UTF-8 byte span as a substring.
        /// </summary>
        private static bool Utf8ContainsReverse(string haystack, ReadOnlySpan<byte> needle)
        {
            if (needle.Length == 0) return true;
            if (needle.Length > haystack.Length) return false;

            // ASCII fast path
            for (var i = 0; i <= haystack.Length - needle.Length; i++)
            {
                if ((byte)haystack[i] == needle[0])
                {
                    var match = true;
                    for (var j = 1; j < needle.Length; j++)
                    {
                        if (haystack[i + j] > 127) goto slowPath;
                        if ((byte)haystack[i + j] != needle[j]) { match = false; break; }
                    }
                    if (match) return true;
                }
            }
            return false;

        slowPath:
            var needleStr = Encoding.UTF8.GetString(needle);
            return haystack.IndexOf(needleStr, StringComparison.Ordinal) >= 0;
        }
    }
}