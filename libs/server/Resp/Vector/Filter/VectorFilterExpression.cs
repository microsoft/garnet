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
        public object Value { get; set; }
    }

    /// <summary>
    /// Represents a member access expression (e.g., .year, .rating).
    /// </summary>
    internal class MemberExpr : Expr
    {
        public string Property { get; set; }
    }

    /// <summary>
    /// Represents a unary operation (e.g., not, -).
    /// </summary>
    internal class UnaryExpr : Expr
    {
        public string Operator { get; set; }
        public Expr Operand { get; set; }
    }

    /// <summary>
    /// Represents a binary operation (e.g., +, -, ==, and).
    /// </summary>
    internal class BinaryExpr : Expr
    {
        public Expr Left { get; set; }
        public string Operator { get; set; }
        public Expr Right { get; set; }
    }
}
