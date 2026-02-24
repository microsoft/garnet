// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text.Json;

namespace Garnet.server.Vector.Filter
{
    /// <summary>
    /// Discriminated union value type to eliminate boxing of doubles/strings
    /// throughout the filter evaluation pipeline.
    /// </summary>
    [StructLayout(LayoutKind.Auto)]
    internal readonly struct FilterValue
    {
        private readonly double _number;
        private readonly string _string;
        private readonly JsonElement _jsonElement;
        private readonly FilterValueKind _kind;

        private FilterValue(double number)
        {
            _number = number;
            _string = null;
            _jsonElement = default;
            _kind = FilterValueKind.Number;
        }

        private FilterValue(string str)
        {
            _number = 0;
            _string = str;
            _jsonElement = default;
            _kind = FilterValueKind.String;
        }

        private FilterValue(JsonElement element)
        {
            _number = 0;
            _string = null;
            _jsonElement = element;
            _kind = FilterValueKind.JsonArray;
        }

        public FilterValueKind Kind
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _kind;
        }

        public bool IsNull
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _kind == FilterValueKind.Null;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public double AsNumber() => _number;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public string AsString() => _string;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public JsonElement AsJsonElement() => _jsonElement;

        public static readonly FilterValue Null = default;
        public static readonly FilterValue True = new(1.0);
        public static readonly FilterValue False = new(0.0);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static FilterValue FromNumber(double value) => new(value);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static FilterValue FromString(string value) => new(value);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static FilterValue FromJsonElement(JsonElement value) => new(value);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static FilterValue FromBool(bool value) => value ? True : False;
    }

    internal enum FilterValueKind : byte
    {
        Null = 0,
        Number = 1,
        String = 2,
        JsonArray = 3,
    }

    /// <summary>
    /// Enum for operator types, replacing string-based operators
    /// to enable integer comparison instead of string comparison on hot paths.
    /// </summary>
    internal enum OperatorKind : byte
    {
        // Arithmetic
        Add,        // +
        Subtract,   // -
        Multiply,   // *
        Divide,     // /
        Modulo,     // %
        Power,      // **

        // Comparison
        GreaterThan,    // >
        LessThan,       // <
        GreaterEqual,   // >=
        LessEqual,      // <=
        Equal,          // ==
        NotEqual,       // !=

        // Logical
        And,        // and, &&
        Or,         // or, ||
        Not,        // not, !

        // Containment
        In,         // in

        // Unary
        Negate,     // - (unary)
    }

    /// <summary>
    /// Base class for filter expression tree nodes.
    /// </summary>
    internal abstract class Expr { }

    /// <summary>
    /// Represents a literal value (number, string, boolean).
    /// Uses FilterValue to avoid boxing.
    /// </summary>
    internal sealed class LiteralExpr : Expr
    {
        public FilterValue Value { get; init; }

        // Keep object-returning property for test compatibility
        public object BoxedValue => Value.Kind switch
        {
            FilterValueKind.Number => Value.AsNumber(),
            FilterValueKind.String => Value.AsString(),
            FilterValueKind.Null => null,
            _ => null
        };
    }

    /// <summary>
    /// Represents a member access expression (e.g., .year, .rating).
    /// </summary>
    internal sealed class MemberExpr : Expr
    {
        public string Property { get; init; }
    }

    /// <summary>
    /// Represents a unary operation (e.g., not, -).
    /// </summary>
    internal sealed class UnaryExpr : Expr
    {
        public OperatorKind Operator { get; init; }
        public Expr Operand { get; init; }
    }

    /// <summary>
    /// Represents a binary operation (e.g., +, -, ==, and).
    /// </summary>
    internal sealed class BinaryExpr : Expr
    {
        public Expr Left { get; init; }
        public OperatorKind Operator { get; init; }
        public Expr Right { get; init; }
    }
}