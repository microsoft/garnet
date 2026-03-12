// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.InteropServices;

namespace Garnet.server
{
    /// <summary>
    /// Token types for the filter expression virtual machine.
    ///
    /// The filter engine uses a stack-based postfix VM
    /// A filter string like <c>.year >= 2000 and .rating > 7</c> is compiled into a flat
    /// array of <see cref="ExprToken"/> instructions in postfix (reverse-Polish) order:
    ///
    /// <code>
    ///   [SEL:year] [NUM:2000] [OP:Gte] [SEL:rating] [NUM:7] [OP:Gt] [OP:And]
    /// </code>
    ///
    /// At execution time, <see cref="ExprRunner"/> walks this array left-to-right:
    /// <list type="bullet">
    ///   <item><description>Value tokens (<see cref="Num"/>, <see cref="Str"/>, <see cref="Tuple"/>,
    ///     <see cref="Null"/>) are pushed onto the evaluation stack.</description></item>
    ///   <item><description><see cref="Selector"/> tokens trigger on-demand JSON field extraction
    ///     via <see cref="AttributeExtractor"/>; the extracted value is pushed.</description></item>
    ///   <item><description><see cref="Op"/> tokens pop 1 or 2 operands, compute the result,
    ///     and push it back.</description></item>
    /// </list>
    ///
    /// After processing all instructions the top-of-stack value is tested for truthiness
    /// to produce the final <c>bool</c> filter result.
    /// </summary>
    internal enum ExprTokenType : byte
    {
        None = 0,
        Num = 1,
        Str = 2,
        Tuple = 3,
        Selector = 4,
        Op = 5,
        Null = 6,
    }

    /// <summary>
    /// Operator opcodes used by the filter expression VM.
    ///
    /// Each opcode has a fixed precedence and arity defined in <see cref="OpTable"/>.
    /// During compilation, <see cref="ExprCompiler"/> uses the shunting-yard algorithm
    /// to reorder operators from infix to postfix based on these precedence values.
    /// During execution, <see cref="ExprRunner"/> pops the required number of operands
    /// (arity), applies the operation, and pushes the result.
    ///
    /// Precedence and semantics match the Redis <c>expr.c ExprOptable[]</c>.
    /// </summary>
    internal enum OpCode : byte
    {
        // Precedence 0
        Or = 0,

        // Precedence 1
        And = 1,

        // Precedence 2
        Gt = 2,
        Gte = 3,
        Lt = 4,
        Lte = 5,
        Eq = 6,
        Neq = 7,
        In = 8,

        // Precedence 3
        Add = 9,
        Sub = 10,

        // Precedence 4
        Mul = 11,
        Div = 12,
        Mod = 13,

        // Precedence 5
        Pow = 14,

        // Precedence 6
        Not = 15,

        // Precedence 7 (markers, not real operators)
        OParen = 16,
        CParen = 17,
    }

    /// <summary>
    /// A 16-byte blittable union token for the filter expression VM.
    ///
    /// All strings are represented as (Utf8Start, Utf8Length) byte-range references into
    /// a source buffer — either the filter expression bytes (for compile-time literals and
    /// selectors) or the JSON bytes (for extracted values). No managed string or array
    /// references, so the struct is GC-free and span-friendly.
    ///
    /// <para><b>Layout (16 bytes, explicit):</b></para>
    /// <code>
    ///   [0]     ExprTokenType  (1 byte)
    ///   [1]     OpCode         (1 byte, overlaps with Flags)
    ///   [8..15] double Num     — OR —
    ///   [8..11] int Utf8Start  + [12..15] int Utf8Length
    /// </code>
    ///
    /// <list type="table">
    ///   <listheader><term>TokenType</term><description>Payload used</description></listheader>
    ///   <item><term>Num</term><description><see cref="Num"/> (double). Booleans are 1.0/0.0.</description></item>
    ///   <item><term>Str</term><description><see cref="Utf8Start"/>+<see cref="Utf8Length"/> into JSON bytes.</description></item>
    ///   <item><term>Selector</term><description><see cref="Utf8Start"/>+<see cref="Utf8Length"/> into filter bytes.</description></item>
    ///   <item><term>Tuple</term><description><see cref="Utf8Start"/> = start index, <see cref="Utf8Length"/> = count
    ///     in the program's <see cref="ExprProgram.TuplePool"/>.</description></item>
    ///   <item><term>Op</term><description><see cref="OpCode"/>.</description></item>
    ///   <item><term>Null</term><description>No payload.</description></item>
    /// </list>
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = 16)]
    internal struct ExprToken
    {
        /// <summary>Discriminator tag.</summary>
        [FieldOffset(0)] public ExprTokenType TokenType;

        /// <summary>Operator opcode — valid when TokenType == Op.</summary>
        [FieldOffset(1)] public OpCode OpCode;

        /// <summary>
        /// Flags byte. Bit 0: HasEscape — the string span contains backslash escapes
        /// that must be handled during comparison.
        /// </summary>
        [FieldOffset(2)] public byte Flags;

        // ── Union payload at offset 8 (8 bytes) ─────────────────────────

        /// <summary>Numeric value. Also used for bool: true=1, false=0.</summary>
        [FieldOffset(8)] public double Num;

        /// <summary>
        /// Start byte-offset of a value in a source buffer.
        /// - Str/Selector: offset into filter bytes or JSON bytes.
        /// - Tuple: start index in <see cref="ExprProgram.TuplePool"/>.
        /// </summary>
        [FieldOffset(8)] public int Utf8Start;

        /// <summary>
        /// Byte-length of a string value, or element count for tuples.
        /// </summary>
        [FieldOffset(12)] public int Utf8Length;

        // ── Flags helpers ────────────────────────────────────────────────

        private const byte HasEscapeFlag = 1;
        private const byte FilterOriginFlag = 2;

        /// <summary>True when this string span contains backslash escapes.</summary>
        public readonly bool HasEscape => (Flags & HasEscapeFlag) != 0;

        /// <summary>
        /// True when this token's byte-range references the filter expression bytes
        /// (compile-time literals and selectors). False when it references JSON bytes
        /// (runtime extracted values).
        /// </summary>
        public readonly bool IsFilterOrigin => (Flags & FilterOriginFlag) != 0;

        // ── Discriminator helpers ────────────────────────────────────────

        /// <summary>True when this token is the default (uninitialized) value.</summary>
        public readonly bool IsNone => TokenType == ExprTokenType.None;

        // ── Factory methods ──────────────────────────────────────────────

        public static ExprToken NewNum(double value)
        {
            var t = default(ExprToken);
            t.TokenType = ExprTokenType.Num;
            t.Num = value;
            return t;
        }

        /// <summary>
        /// Create a string token referencing raw UTF-8 bytes in the JSON buffer.
        /// Zero allocation. The offset and length define the string content (excluding quotes).
        /// </summary>
        public static ExprToken NewStr(int utf8Start, int utf8Length, bool hasEscape = false)
        {
            var t = default(ExprToken);
            t.TokenType = ExprTokenType.Str;
            t.Utf8Start = utf8Start;
            t.Utf8Length = utf8Length;
            if (hasEscape) t.Flags = HasEscapeFlag;
            return t;
        }

        /// <summary>
        /// Create a string token referencing raw UTF-8 bytes in the filter expression buffer.
        /// Used by the compiler for string literals. The <see cref="IsFilterOrigin"/> flag
        /// tells the runner to resolve this token against filter bytes, not JSON bytes.
        /// </summary>
        public static ExprToken NewFilterStr(int utf8Start, int utf8Length, bool hasEscape = false)
        {
            var t = default(ExprToken);
            t.TokenType = ExprTokenType.Str;
            t.Utf8Start = utf8Start;
            t.Utf8Length = utf8Length;
            t.Flags = FilterOriginFlag;
            if (hasEscape) t.Flags |= HasEscapeFlag;
            return t;
        }

        /// <summary>
        /// Create a selector token referencing a field name in the filter bytes.
        /// </summary>
        public static ExprToken NewSelector(int utf8Start, int utf8Length)
        {
            var t = default(ExprToken);
            t.TokenType = ExprTokenType.Selector;
            t.Utf8Start = utf8Start;
            t.Utf8Length = utf8Length;
            return t;
        }

        public static ExprToken NewOp(OpCode opCode)
        {
            var t = default(ExprToken);
            t.TokenType = ExprTokenType.Op;
            t.OpCode = opCode;
            return t;
        }

        public static ExprToken NewNull()
        {
            var t = default(ExprToken);
            t.TokenType = ExprTokenType.Null;
            return t;
        }

        private const byte RuntimeTupleFlag = 4;

        /// <summary>
        /// Create a tuple token indexing into <see cref="ExprProgram.TuplePool"/> (compile-time).
        /// </summary>
        public static ExprToken NewTuple(int poolStart, int poolLength)
        {
            var t = default(ExprToken);
            t.TokenType = ExprTokenType.Tuple;
            t.Utf8Start = poolStart;
            t.Utf8Length = poolLength;
            return t;
        }

        /// <summary>
        /// Create a tuple token indexing into <see cref="ExprProgram.RuntimePool"/> (extracted JSON arrays).
        /// </summary>
        public static ExprToken NewRuntimeTuple(int poolStart, int poolLength)
        {
            var t = default(ExprToken);
            t.TokenType = ExprTokenType.Tuple;
            t.Utf8Start = poolStart;
            t.Utf8Length = poolLength;
            t.Flags = RuntimeTupleFlag;
            return t;
        }

        /// <summary>True when this is a runtime-extracted tuple (from JSON array, not compile-time).</summary>
        public readonly bool IsRuntimeTuple => (Flags & RuntimeTupleFlag) != 0;
    }

    /// <summary>
    /// Operator metadata table, mirroring Redis ExprOptable.
    /// Provides precedence and arity lookup for shunting-yard compilation.
    /// </summary>
    internal static class OpTable
    {
        // No static constructor — JIT can initialize lazily (beforefieldinit).
        // Indexed by OpCode for O(1) lookup.
        private static readonly (int Precedence, int Arity)[] Table =
        [
            (0, 2), // Or       = 0
            (1, 2), // And      = 1
            (2, 2), // Gt       = 2
            (2, 2), // Gte      = 3
            (2, 2), // Lt       = 4
            (2, 2), // Lte      = 5
            (2, 2), // Eq       = 6
            (2, 2), // Neq      = 7
            (2, 2), // In       = 8
            (3, 2), // Add      = 9
            (3, 2), // Sub      = 10
            (4, 2), // Mul      = 11
            (4, 2), // Div      = 12
            (4, 2), // Mod      = 13
            (5, 2), // Pow      = 14
            (6, 1), // Not      = 15
            (7, 0), // OParen   = 16
            (7, 0), // CParen   = 17
        ];

        public static int GetPrecedence(OpCode code) => Table[(int)code].Precedence;

        public static int GetArity(OpCode code) => Table[(int)code].Arity;
    }

    /// <summary>
    /// Compiled filter expression program — the output of
    /// <see cref="ExprCompiler.TryCompile(ReadOnlySpan{byte}, out int)"/>
    /// and the input to <c>ExprRunner.Run</c>.
    ///
    /// Contains a flat postfix instruction sequence and the source bytes that string/selector
    /// tokens reference. All <see cref="ExprToken"/> values in the program are byte-range
    /// references into the caller-provided filter bytes (compile-time) or the per-candidate JSON
    /// bytes (runtime).
    ///
    /// <para><b>Compile-once, run-many:</b> The program is compiled once per query, then
    /// executed against every candidate element's raw JSON bytes. The program itself is
    /// read-only during execution.</para>
    /// </summary>
    internal sealed class ExprProgram
    {
        /// <summary>The compiled postfix instruction sequence.</summary>
        public ExprToken[] Instructions;

        /// <summary>Number of instructions in the program.</summary>
        public int Length;

        /// <summary>
        /// Flat pool of tuple element tokens. Tuple tokens in <see cref="Instructions"/>
        /// store (StartIndex, Count) into this array.
        /// </summary>
        public ExprToken[] TuplePool;

        /// <summary>Number of elements used in <see cref="TuplePool"/>.</summary>
        public int TuplePoolLength;

        /// <summary>
        /// Runtime tuple pool for extracted JSON array elements.
        /// Reused across candidate evaluations. Runtime elements are appended
        /// and reset before each candidate evaluation via <see cref="ResetRuntimePool"/>.
        /// </summary>
        public ExprToken[] RuntimePool;

        /// <summary>Current write position in <see cref="RuntimePool"/>.</summary>
        public int RuntimePoolLength;

        /// <summary>
        /// Append elements to the runtime pool and return a Tuple token.
        /// The pool auto-grows if needed.
        /// </summary>
        public ExprToken AppendRuntimeTuple(ExprToken[] elements, int count)
        {
            if (count == 0)
                return ExprToken.NewTuple(0, 0);

            RuntimePool ??= new ExprToken[64];
            if (RuntimePoolLength + count > RuntimePool.Length)
                System.Array.Resize(ref RuntimePool, System.Math.Max(RuntimePool.Length * 2, RuntimePoolLength + count));

            var start = RuntimePoolLength;
            System.Array.Copy(elements, 0, RuntimePool, start, count);
            RuntimePoolLength += count;
            return ExprToken.NewRuntimeTuple(start, count);
        }

        /// <summary>Reset the runtime pool for a new candidate evaluation.</summary>
        public void ResetRuntimePool() => RuntimePoolLength = 0;

        /// <summary>Cached unique selector byte-ranges (deduplicated).</summary>
        private (int Start, int Length)[] selectorRanges;

        /// <summary>
        /// Get the unique selector (field name) byte-ranges referenced by this program.
        /// Each range is an (offset, length) into the caller-provided filter bytes.
        /// Cached after first call.
        /// </summary>
        public (int Start, int Length)[] GetSelectorRanges(ReadOnlySpan<byte> filterBytes)
        {
            if (selectorRanges != null)
                return selectorRanges;

            // Deduplicate by content
            var count = 0;
            var temp = new (int Start, int Length)[Length]; // upper bound
            for (var i = 0; i < Length; i++)
            {
                if (Instructions[i].TokenType != ExprTokenType.Selector)
                    continue;

                var start = Instructions[i].Utf8Start;
                var len = Instructions[i].Utf8Length;
                var span = filterBytes.Slice(start, len);
                var found = false;
                for (var j = 0; j < count; j++)
                {
                    if (filterBytes.Slice(temp[j].Start, temp[j].Length).SequenceEqual(span))
                    {
                        found = true;
                        break;
                    }
                }
                if (!found)
                    temp[count++] = (start, len);
            }

            selectorRanges = new (int, int)[count];
            System.Array.Copy(temp, selectorRanges, count);
            return selectorRanges;
        }
    }
}
