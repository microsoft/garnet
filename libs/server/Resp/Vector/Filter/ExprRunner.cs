// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Globalization;
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
        private const int MaxStack = 256;

        /// <summary>
        /// Execute the compiled program against JSON attribute data.
        /// Returns true if the expression evaluates to a truthy value, false otherwise.
        /// Returns false if the JSON is malformed or a selector cannot be resolved.
        /// </summary>
        public static bool Run(ExprProgram program, ReadOnlySpan<byte> json)
        {
            // Stack for values during execution
            var stack = new ExprToken[MaxStack];
            var stackLen = 0;

            for (var i = 0; i < program.Length; i++)
            {
                var inst = program.Instructions[i];

                // Selectors — extract field from JSON
                if (inst.TokenType == ExprTokenType.Selector)
                {
                    var extracted = AttributeExtractor.ExtractField(json, inst.Str);
                    if (extracted == null)
                        return false; // Selector not found → expression is false (matches Redis)

                    if (stackLen >= MaxStack) return false;
                    stack[stackLen++] = extracted;
                    continue;
                }

                // Non-operator values — push directly
                if (inst.TokenType != ExprTokenType.Op)
                {
                    if (stackLen >= MaxStack) return false;
                    stack[stackLen++] = inst;
                    continue;
                }

                // Operators — pop operands, compute, push result
                var arity = OpTable.GetArity(inst.OpCode);
                if (stackLen < arity) return false;

                ExprToken b = stackLen > 0 ? stack[--stackLen] : null;
                ExprToken a = arity == 2 && stackLen > 0 ? stack[--stackLen] : null;

                var result = ExprToken.NewNum(0);

                switch (inst.OpCode)
                {
                    case OpCode.Not:
                        result.Num = ToBool(b) == 0 ? 1 : 0;
                        break;
                    case OpCode.Pow:
                        result.Num = Math.Pow(ToNum(a), ToNum(b));
                        break;
                    case OpCode.Mul:
                        result.Num = ToNum(a) * ToNum(b);
                        break;
                    case OpCode.Div:
                        result.Num = ToNum(a) / ToNum(b);
                        break;
                    case OpCode.Mod:
                        result.Num = ToNum(a) % ToNum(b);
                        break;
                    case OpCode.Add:
                        result.Num = ToNum(a) + ToNum(b);
                        break;
                    case OpCode.Sub:
                        result.Num = ToNum(a) - ToNum(b);
                        break;
                    case OpCode.Gt:
                        result.Num = ToNum(a) > ToNum(b) ? 1 : 0;
                        break;
                    case OpCode.Gte:
                        result.Num = ToNum(a) >= ToNum(b) ? 1 : 0;
                        break;
                    case OpCode.Lt:
                        result.Num = ToNum(a) < ToNum(b) ? 1 : 0;
                        break;
                    case OpCode.Lte:
                        result.Num = ToNum(a) <= ToNum(b) ? 1 : 0;
                        break;
                    case OpCode.Eq:
                        result.Num = AreEqual(a, b) ? 1 : 0;
                        break;
                    case OpCode.Neq:
                        result.Num = !AreEqual(a, b) ? 1 : 0;
                        break;
                    case OpCode.In:
                        result.Num = EvalIn(a, b) ? 1 : 0;
                        break;
                    case OpCode.And:
                        result.Num = ToBool(a) != 0 && ToBool(b) != 0 ? 1 : 0;
                        break;
                    case OpCode.Or:
                        result.Num = ToBool(a) != 0 || ToBool(b) != 0 ? 1 : 0;
                        break;
                }

                if (stackLen >= MaxStack) return false;
                stack[stackLen++] = result;
            }

            if (stackLen == 0) return false;
            return ToBool(stack[stackLen - 1]) != 0;
        }

        // ======================== Type conversion helpers ========================

        /// <summary>
        /// Convert a token to its numeric value.
        /// Strings are parsed as numbers; unparseable strings return 0.
        /// Matches Redis exprTokenToNum().
        /// </summary>
        private static double ToNum(ExprToken t)
        {
            if (t == null) return 0;
            if (t.TokenType == ExprTokenType.Num) return t.Num;
            if (t.TokenType == ExprTokenType.Str && t.Str != null)
            {
                return double.TryParse(t.Str, NumberStyles.Float | NumberStyles.AllowLeadingSign,
                    CultureInfo.InvariantCulture, out var result) ? result : 0;
            }
            return 0;
        }

        /// <summary>
        /// Convert a token to boolean (0 or 1).
        /// Matches Redis exprTokenToBool(): null=0, num!=0=1, empty string=0, else=1.
        /// </summary>
        private static double ToBool(ExprToken t)
        {
            if (t == null) return 0;
            if (t.TokenType == ExprTokenType.Num) return t.Num != 0 ? 1 : 0;
            if (t.TokenType == ExprTokenType.Str && (t.Str == null || t.Str.Length == 0)) return 0;
            if (t.TokenType == ExprTokenType.Null) return 0;
            return 1; // Non-empty strings, tuples, etc. are truthy
        }

        /// <summary>
        /// Compare two tokens for equality.
        /// Matches Redis exprTokensEqual():
        /// - Both strings → exact string comparison
        /// - Both numbers → exact numeric equality (no epsilon)
        /// - One/both null → equal only if both null
        /// - Mixed types → coerce to numbers and compare
        /// </summary>
        private static bool AreEqual(ExprToken a, ExprToken b)
        {
            if (a == null || b == null) return a == null && b == null;

            // Both strings
            if (a.TokenType == ExprTokenType.Str && b.TokenType == ExprTokenType.Str)
                return string.Equals(a.Str, b.Str, StringComparison.Ordinal);

            // Both numbers
            if (a.TokenType == ExprTokenType.Num && b.TokenType == ExprTokenType.Num)
                return a.Num == b.Num; // Exact comparison, matching Redis

            // One/both null
            if (a.TokenType == ExprTokenType.Null || b.TokenType == ExprTokenType.Null)
                return a.TokenType == b.TokenType;

            // Mixed types — coerce to number
            return ToNum(a) == ToNum(b);
        }

        /// <summary>
        /// Evaluate the IN operator.
        /// Matches Redis expr.c behavior:
        /// 1. If b is a Tuple, check membership (element-wise AreEqual)
        /// 2. If both a and b are strings, check substring containment
        /// 3. Otherwise, false
        /// </summary>
        private static bool EvalIn(ExprToken a, ExprToken b)
        {
            if (b == null) return false;

            // Tuple membership (works for both expression tuples [1,2,3] and JSON array tuples)
            if (b.TokenType == ExprTokenType.Tuple)
            {
                for (var i = 0; i < b.TupleLength; i++)
                {
                    if (AreEqual(a, b.TupleElements[i]))
                        return true;
                }
                return false;
            }

            // String substring check (matching Redis exprTokensStringIn)
            if (a != null && a.TokenType == ExprTokenType.Str && b.TokenType == ExprTokenType.Str)
            {
                if (a.Str == null || b.Str == null) return false;
                if (a.Str.Length > b.Str.Length) return false;
                return b.Str.IndexOf(a.Str, StringComparison.Ordinal) >= 0;
            }

            return false;
        }
    }
}