// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server.Vector.Filter
{
    /// <summary>
    /// Base class for filter expression tree nodes.
    /// </summary>
    internal abstract class Expr { }

    /// <summary>
    /// Represents a literal value (number, string, boolean).
    /// </summary>
    internal class LiteralExpr : Expr
    {
        public object Value { get; init; }
    }

    /// <summary>
    /// Represents a member access expression (e.g., .year, .rating).
    /// </summary>
    internal class MemberExpr : Expr
    {
        public string Property { get; init; }
    }

    /// <summary>
    /// Represents a unary operation (e.g., not, -).
    /// </summary>
    internal class UnaryExpr : Expr
    {
        public string Operator { get; init; }
        public Expr Operand { get; init; }
    }

    /// <summary>
    /// Represents a binary operation (e.g., +, -, ==, and).
    /// </summary>
    internal class BinaryExpr : Expr
    {
        public Expr Left { get; init; }
        public string Operator { get; init; }
        public Expr Right { get; init; }
    }
}
