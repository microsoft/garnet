// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server.Vector.Filter
{
    /// <summary>
    /// Token types for the filter expression virtual machine.
    ///
    /// The filter engine uses a stack-based postfix VM (modeled after Redis <c>expr.c</c>).
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
    /// A token in the compiled filter program or on the evaluation stack.
    ///
    /// Designed after Redis <c>expr.c exprtoken</c> — a single type that can represent any
    /// value the VM needs:
    ///
    /// <list type="table">
    ///   <listheader><term>TokenType</term><description>Payload used</description></listheader>
    ///   <item><term><see cref="ExprTokenType.Num"/></term>
    ///     <description><see cref="Num"/> — <c>double</c> (booleans are <c>1</c>/<c>0</c>).</description></item>
    ///   <item><term><see cref="ExprTokenType.Str"/></term>
    ///     <description><see cref="Str"/> — an interned or extracted <c>string</c>.</description></item>
    ///   <item><term><see cref="ExprTokenType.Selector"/></term>
    ///     <description><see cref="Str"/> — the JSON field name (e.g. <c>"year"</c> from <c>.year</c>).</description></item>
    ///   <item><term><see cref="ExprTokenType.Tuple"/></term>
    ///     <description><see cref="TupleElements"/> + <see cref="TupleLength"/> — for the
    ///     <c>in</c> operator or JSON array values.</description></item>
    ///   <item><term><see cref="ExprTokenType.Op"/></term>
    ///     <description><see cref="OpCode"/> — the operator to execute.</description></item>
    ///   <item><term><see cref="ExprTokenType.Null"/></term>
    ///     <description>No payload — represents JSON <c>null</c> or the <c>null</c> keyword.</description></item>
    /// </list>
    ///
    /// <para><b>Lifetime:</b> Tokens inside the compiled <see cref="ExprProgram"/> are
    /// allocated once and reused across all candidate evaluations. Tokens created during
    /// execution (e.g. from <see cref="AttributeExtractor"/> JSON field extraction) are
    /// transient and discarded after each <c>ExprRunner.Run</c> call.</para>
    /// </summary>
    internal struct ExprToken
    {
        public ExprTokenType TokenType;

        /// <summary>Numeric value. Also used for bool: true=1, false=0.</summary>
        public double Num;

        /// <summary>String value — for Str and Selector types.</summary>
        public string Str;

        /// <summary>Operator opcode — for Op type.</summary>
        public OpCode OpCode;

        /// <summary>Tuple elements for IN operator.</summary>
        public ExprToken[] TupleElements;

        /// <summary>Number of elements in the tuple.</summary>
        public int TupleLength;

        /// <summary>Start byte-offset of a string value in the source JSON (for zero-allocation extraction).</summary>
        public int Utf8Start;

        /// <summary>Byte-length of the string value in the source JSON (for zero-allocation extraction).</summary>
        public int Utf8Length;

        /// <summary>True when this token is the default (uninitialized) value, replacing null checks.</summary>
        public readonly bool IsNone => TokenType == ExprTokenType.None;

        /// <summary>True when this is a Str token that references raw JSON bytes instead of an allocated string.</summary>
        public readonly bool IsJsonRef => TokenType == ExprTokenType.Str && Str == null;

        public static ExprToken NewNum(double value)
        {
            return new ExprToken { TokenType = ExprTokenType.Num, Num = value };
        }

        public static ExprToken NewStr(string value)
        {
            return new ExprToken { TokenType = ExprTokenType.Str, Str = value };
        }

        public static ExprToken NewSelector(string fieldName)
        {
            return new ExprToken { TokenType = ExprTokenType.Selector, Str = fieldName };
        }

        public static ExprToken NewOp(OpCode opCode)
        {
            return new ExprToken { TokenType = ExprTokenType.Op, OpCode = opCode };
        }

        public static ExprToken NewNull()
        {
            return new ExprToken { TokenType = ExprTokenType.Null };
        }

        public static ExprToken NewTuple(ExprToken[] elements, int length)
        {
            return new ExprToken { TokenType = ExprTokenType.Tuple, TupleElements = elements, TupleLength = length };
        }

        /// <summary>
        /// Create a string token that references raw UTF-8 bytes in the source JSON — zero allocation.
        /// The offset and length define the string content (excluding quotes) within the JSON span.
        /// </summary>
        public static ExprToken NewJsonStr(int utf8Start, int utf8Length)
        {
            return new ExprToken { TokenType = ExprTokenType.Str, Utf8Start = utf8Start, Utf8Length = utf8Length };
        }
    }

    /// <summary>
    /// Operator metadata table, mirroring Redis ExprOptable.
    /// Provides precedence and arity lookup for shunting-yard compilation.
    /// </summary>
    internal static class OpTable
    {
        // Indexed by OpCode for O(1) lookup.
        // Entries: (Precedence, Arity). OpCode enum values are consecutive 0..17.
        private static readonly (int Precedence, int Arity)[] Table;

        static OpTable()
        {
            Table = new (int, int)[18];
            Table[(int)OpCode.Or] = (0, 2);
            Table[(int)OpCode.And] = (1, 2);
            Table[(int)OpCode.Gt] = (2, 2);
            Table[(int)OpCode.Gte] = (2, 2);
            Table[(int)OpCode.Lt] = (2, 2);
            Table[(int)OpCode.Lte] = (2, 2);
            Table[(int)OpCode.Eq] = (2, 2);
            Table[(int)OpCode.Neq] = (2, 2);
            Table[(int)OpCode.In] = (2, 2);
            Table[(int)OpCode.Add] = (3, 2);
            Table[(int)OpCode.Sub] = (3, 2);
            Table[(int)OpCode.Mul] = (4, 2);
            Table[(int)OpCode.Div] = (4, 2);
            Table[(int)OpCode.Mod] = (4, 2);
            Table[(int)OpCode.Pow] = (5, 2);
            Table[(int)OpCode.Not] = (6, 1);
            Table[(int)OpCode.OParen] = (7, 0);
            Table[(int)OpCode.CParen] = (7, 0);
        }

        public static int GetPrecedence(OpCode code) => Table[(int)code].Precedence;

        public static int GetArity(OpCode code) => Table[(int)code].Arity;
    }

    /// <summary>
    /// Compiled filter expression program — the output of <see cref="ExprCompiler.TryCompile"/>
    /// and the input to <c>ExprRunner.Run</c>.
    ///
    /// Contains a flat postfix (reverse-Polish notation) instruction sequence where every
    /// element is an <see cref="ExprToken"/>:
    ///
    /// <code>
    ///   Source:   .year >= 2000 and .rating > 7
    ///   Program:  [SEL:year] [NUM:2000] [OP:>=] [SEL:rating] [NUM:7] [OP:>] [OP:and]
    /// </code>
    ///
    /// <para><b>Compile-once, run-many:</b> The program is compiled once per query, then
    /// executed against every candidate element's raw JSON bytes. The program itself is
    /// read-only during execution — all mutable state lives in the per-call evaluation
    /// stack inside <see cref="ExprRunner"/>.</para>
    ///
    /// <para>This is the C# equivalent of the <c>exprstate.program[]</c> array in
    /// Redis <c>expr.c</c>. The evaluation stack (<c>values_stack</c> in Redis) is
    /// <em>not</em> stored here — it is allocated per-call in <c>ExprRunner.Run</c>.</para>
    /// </summary>
    internal sealed class ExprProgram
    {
        /// <summary>The compiled postfix instruction sequence.</summary>
        public ExprToken[] Instructions;

        /// <summary>Number of instructions in the program.</summary>
        public int Length;

        /// <summary>Cached unique selector names (field names) used in this program.</summary>
        private string[] selectorNames;

        /// <summary>
        /// Get the unique selector (field) names referenced by this program.
        /// Cached after first call — safe to call repeatedly.
        /// </summary>
        public string[] GetSelectors()
        {
            if (selectorNames != null)
                return selectorNames;

            // Count unique selectors
            var seen = new System.Collections.Generic.HashSet<string>(System.StringComparer.Ordinal);
            for (var i = 0; i < Length; i++)
            {
                if (Instructions[i].TokenType == ExprTokenType.Selector)
                    seen.Add(Instructions[i].Str);
            }

            var names = new string[seen.Count];
            seen.CopyTo(names);
            selectorNames = names;
            return selectorNames;
        }
    }
}