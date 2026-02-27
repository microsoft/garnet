// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Text;
using Garnet.server.Vector.Filter;

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
            var program = ExprCompiler.TryCompile(Encoding.UTF8.GetBytes(expression), out var errpos);
            if (program == null)
                throw new InvalidOperationException($"Compilation failed at position {errpos}");

            // For single-value expressions (no selectors), run returns bool.
            // To get the actual value, we use RunAndReturnTop.
            var jsonBytes = Encoding.UTF8.GetBytes(json);
            return RunAndReturnTop(program, jsonBytes);
        }

        /// <summary>
        /// Compile and run a filter expression against JSON, returning a boolean result.
        /// </summary>
        internal static bool EvaluateFilterTruthy(string expression, string json)
        {
            var program = ExprCompiler.TryCompile(Encoding.UTF8.GetBytes(expression), out var errpos);
            if (program == null)
                throw new InvalidOperationException($"Compilation failed at position {errpos}");

            var jsonBytes = Encoding.UTF8.GetBytes(json);
            return ExprRunner.Run(program, jsonBytes);
        }

        /// <summary>
        /// Try to compile a filter expression. Returns true on success.
        /// </summary>
        internal static bool TryCompile(string expression, out ExprProgram program)
        {
            program = ExprCompiler.TryCompile(Encoding.UTF8.GetBytes(expression), out _);
            return program != null;
        }

        /// <summary>
        /// Execute a compiled program and return the top-of-stack value (for testing).
        /// This is a test-only method that mirrors ExprRunner.Run but returns the raw result
        /// instead of a boolean, so tests can inspect numeric/string values.
        /// </summary>
        private static ExprToken RunAndReturnTop(ExprProgram program, ReadOnlySpan<byte> json)
        {
            var stack = new ExprToken[256];
            var stackLen = 0;

            for (var i = 0; i < program.Length; i++)
            {
                var inst = program.Instructions[i];

                if (inst.TokenType == ExprTokenType.Selector)
                {
                    var extracted = AttributeExtractor.ExtractField(json, inst.Str);
                    if (extracted == null)
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
                ExprToken b = stackLen > 0 ? stack[--stackLen] : null;
                ExprToken a = arity == 2 && stackLen > 0 ? stack[--stackLen] : null;

                var result = ExprToken.NewNum(0);

                switch (inst.OpCode)
                {
                    case OpCode.Not:
                        result.Num = TokenToBool(b) == 0 ? 1 : 0;
                        break;
                    case OpCode.Pow:
                        result.Num = Math.Pow(TokenToNum(a), TokenToNum(b));
                        break;
                    case OpCode.Mul:
                        result.Num = TokenToNum(a) * TokenToNum(b);
                        break;
                    case OpCode.Div:
                        result.Num = TokenToNum(a) / TokenToNum(b);
                        break;
                    case OpCode.Mod:
                        result.Num = TokenToNum(a) % TokenToNum(b);
                        break;
                    case OpCode.Add:
                        result.Num = TokenToNum(a) + TokenToNum(b);
                        break;
                    case OpCode.Sub:
                        result.Num = TokenToNum(a) - TokenToNum(b);
                        break;
                    case OpCode.Gt:
                        result.Num = TokenToNum(a) > TokenToNum(b) ? 1 : 0;
                        break;
                    case OpCode.Gte:
                        result.Num = TokenToNum(a) >= TokenToNum(b) ? 1 : 0;
                        break;
                    case OpCode.Lt:
                        result.Num = TokenToNum(a) < TokenToNum(b) ? 1 : 0;
                        break;
                    case OpCode.Lte:
                        result.Num = TokenToNum(a) <= TokenToNum(b) ? 1 : 0;
                        break;
                    case OpCode.Eq:
                        result.Num = TokensEqual(a, b) ? 1 : 0;
                        break;
                    case OpCode.Neq:
                        result.Num = !TokensEqual(a, b) ? 1 : 0;
                        break;
                    case OpCode.In:
                        result.Num = EvalIn(a, b) ? 1 : 0;
                        break;
                    case OpCode.And:
                        result.Num = TokenToBool(a) != 0 && TokenToBool(b) != 0 ? 1 : 0;
                        break;
                    case OpCode.Or:
                        result.Num = TokenToBool(a) != 0 || TokenToBool(b) != 0 ? 1 : 0;
                        break;
                }

                stack[stackLen++] = result;
            }

            return stackLen > 0 ? stack[stackLen - 1] : ExprToken.NewNull();
        }

        private static double TokenToNum(ExprToken t)
        {
            if (t == null) return 0;
            if (t.TokenType == ExprTokenType.Num) return t.Num;
            if (t.TokenType == ExprTokenType.Str && t.Str != null)
            {
                return double.TryParse(t.Str, System.Globalization.NumberStyles.Float | System.Globalization.NumberStyles.AllowLeadingSign,
                    System.Globalization.CultureInfo.InvariantCulture, out var result) ? result : 0;
            }
            return 0;
        }

        private static double TokenToBool(ExprToken t)
        {
            if (t == null) return 0;
            if (t.TokenType == ExprTokenType.Num) return t.Num != 0 ? 1 : 0;
            if (t.TokenType == ExprTokenType.Str && (t.Str == null || t.Str.Length == 0)) return 0;
            if (t.TokenType == ExprTokenType.Null) return 0;
            return 1;
        }

        private static bool TokensEqual(ExprToken a, ExprToken b)
        {
            if (a == null || b == null) return a == null && b == null;
            if (a.TokenType == ExprTokenType.Str && b.TokenType == ExprTokenType.Str)
                return string.Equals(a.Str, b.Str, StringComparison.Ordinal);
            if (a.TokenType == ExprTokenType.Num && b.TokenType == ExprTokenType.Num)
                return a.Num == b.Num;
            if (a.TokenType == ExprTokenType.Null || b.TokenType == ExprTokenType.Null)
                return a.TokenType == b.TokenType;
            return TokenToNum(a) == TokenToNum(b);
        }

        private static bool EvalIn(ExprToken a, ExprToken b)
        {
            if (b == null) return false;
            if (b.TokenType == ExprTokenType.Tuple)
            {
                for (var i = 0; i < b.TupleLength; i++)
                {
                    if (TokensEqual(a, b.TupleElements[i]))
                        return true;
                }
                return false;
            }
            if (a != null && a.TokenType == ExprTokenType.Str && b.TokenType == ExprTokenType.Str)
            {
                if (a.Str == null || b.Str == null) return false;
                return b.Str.IndexOf(a.Str, StringComparison.Ordinal) >= 0;
            }
            return false;
        }
    }
}