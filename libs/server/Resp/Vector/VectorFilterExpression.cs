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
    /// A 16-byte blittable union token — the universal data type for the filter expression VM.
    ///
    /// <para><b>Design goals:</b></para>
    /// <list type="bullet">
    ///   <item><description><b>Zero allocation</b> — no managed references (string, array, object).
    ///     Safe for <c>stackalloc</c>, <c>Span&lt;ExprToken&gt;</c>, and contiguous buffers.</description></item>
    ///   <item><description><b>Fixed 16-byte size</b> — power-of-two for optimal array indexing
    ///     (<c>index &lt;&lt; 4</c>). Four tokens fit exactly in one 64-byte cache line.</description></item>
    ///   <item><description><b>Tagged union</b> — <see cref="TokenType"/> discriminates which
    ///     payload fields are active. Only one interpretation of the union is valid at a time.</description></item>
    ///   <item><description><b>Zero-copy strings</b> — string values are (offset, length) byte-range
    ///     references into an external buffer, not copies. The <see cref="Flags"/> byte tracks
    ///     which buffer (filter vs JSON) and whether escape handling is needed.</description></item>
    /// </list>
    ///
    /// <para><b>Memory layout (16 bytes, explicit):</b></para>
    /// <code>
    ///   ExprToken — [StructLayout(LayoutKind.Explicit, Size = 16)]
    ///
    ///   Byte offset:  0    1    2    3    4    5    6    7    8    9   10   11   12   13   14   15
    ///                ┌────┬────┬────┬────┬────┬────┬────┬────┬────┬────┬────┬────┬────┬────┬────┬────┐
    ///                │Type│ Op │Flag│         padding (5 bytes)   │       Union payload (8 bytes)     │
    ///                │Code│Code│  s │                             │                                   │
    ///                └────┴────┴────┴────────────────────────────┴───────────────────────────────────┘
    ///                ◄─── header (8 bytes) ──────────────────────►◄─── payload (8 bytes) ───────────►
    ///
    ///   The payload at offset 8 is a UNION (overlapping fields):
    ///     ● double Num       [8..15]  — 8 bytes, for numbers and booleans
    ///     ● int Utf8Start    [8..11]  — 4 bytes ┐ for strings, selectors, tuples
    ///     ● int Utf8Length   [12..15] — 4 bytes ┘
    ///
    ///   Header fields:
    ///     [0] TokenType   — discriminator tag (ExprTokenType enum)
    ///     [1] OpCode      — operator opcode (valid when TokenType == Op)
    ///     [2] Flags       — bitfield:
    ///                         bit 0 (0x01): HasEscape    — string contains \", \\, \n etc.
    ///                         bit 1 (0x02): FilterOrigin — bytes reference filter, not JSON
    ///                         bit 2 (0x04): RuntimeTuple — tuple in RuntimePool, not TuplePool
    ///                         bits 3–7:     Reserved for future use
    ///     [3..7]          — padding (ensures 8-byte alignment for the double in the union)
    /// </code>
    ///
    /// <para><b>Payload usage by token type:</b></para>
    /// <list type="table">
    ///   <listheader><term>TokenType</term><description>Payload interpretation</description></listheader>
    ///   <item><term>Num</term><description><see cref="Num"/> (double). Booleans are 1.0/0.0.</description></item>
    ///   <item><term>Str</term><description><see cref="Utf8Start"/>+<see cref="Utf8Length"/> — byte range into
    ///     JSON bytes (default) or filter bytes (when <see cref="IsFilterOrigin"/> is set).
    ///     If <see cref="HasEscape"/> is set, the span contains JSON escape sequences that
    ///     require unescape-aware comparison.</description></item>
    ///   <item><term>Selector</term><description><see cref="Utf8Start"/>+<see cref="Utf8Length"/> — field name
    ///     byte range into filter bytes (e.g. "year" from ".year &gt; 2000"). Always FilterOrigin.</description></item>
    ///   <item><term>Tuple</term><description><see cref="Utf8Start"/> = start index, <see cref="Utf8Length"/> = count
    ///     in either <see cref="ExprProgram.TuplePool"/> (compile-time <c>[1, "x", 3]</c> literals)
    ///     or <see cref="ExprProgram.RuntimePool"/> (JSON array extraction, when <see cref="IsRuntimeTuple"/>).</description></item>
    ///   <item><term>Op</term><description><see cref="OpCode"/> identifies the operation. No payload used.</description></item>
    ///   <item><term>Null</term><description>No payload. Represents JSON null or missing fields.</description></item>
    ///   <item><term>None</term><description>Default/uninitialized. All bytes zero. Used as sentinel.</description></item>
    /// </list>
    ///
    /// <para><b>Factory methods:</b> All construction goes through static methods (<see cref="NewNum"/>,
    /// <see cref="NewStr"/>, <see cref="NewFilterStr"/>, <see cref="NewSelector"/>, <see cref="NewOp"/>,
    /// <see cref="NewNull"/>, <see cref="NewTuple"/>, <see cref="NewRuntimeTuple"/>) that initialize
    /// from <c>default</c> (all-zeros) and set only the relevant fields. This ensures padding bytes
    /// are always zero.</para>
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = Size)]
    internal struct ExprToken
    {
        /// <summary>Size of this struct in bytes.</summary>
        public const int Size = 16;

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
    /// Compiled filter expression program — zero-allocation ref struct.
    /// All storage is caller-provided via <see cref="Span{ExprToken}"/> buffers
    /// (typically stackalloc'd). The program does not own any heap memory.
    /// </summary>
    internal ref struct ExprProgram
    {
        /// <summary>The compiled postfix instruction sequence.</summary>
        public Span<ExprToken> Instructions;

        /// <summary>Number of instructions in the program.</summary>
        public int Length;

        /// <summary>
        /// Flat pool of tuple element tokens. Tuple tokens in <see cref="Instructions"/>
        /// store (StartIndex, Count) into this span.
        /// </summary>
        public Span<ExprToken> TuplePool;

        /// <summary>Number of elements used in <see cref="TuplePool"/>.</summary>
        public int TuplePoolLength;

        /// <summary>
        /// Runtime tuple pool for extracted JSON array elements.
        /// Reused across candidate evaluations. Runtime elements are appended
        /// and reset before each candidate evaluation via <see cref="ResetRuntimePool"/>.
        /// </summary>
        public Span<ExprToken> RuntimePool;

        /// <summary>Current write position in <see cref="RuntimePool"/>.</summary>
        public int RuntimePoolLength;

        /// <summary>Reset the runtime pool for a new candidate evaluation.</summary>
        public void ResetRuntimePool() => RuntimePoolLength = 0;
    }
}