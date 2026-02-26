// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using Allure.NUnit;
using Garnet.server.Vector.Filter;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    [AllureNUnit]
    [TestFixture]
    public class VectorFilterParserTests : AllureTestBase
    {
        [Test]
        public void Parser_NumberLiteral()
        {
            ClassicAssert.IsTrue(VectorFilterTokenizer.TryTokenize("42", out var tokens, out _));
            ClassicAssert.IsTrue(VectorFilterParser.TryParseExpression(tokens, 0, out var expr, out var end, out _));
            ClassicAssert.AreEqual(1, end);
            ClassicAssert.IsInstanceOf<LiteralExpr>(expr);
            var lit = (LiteralExpr)expr;
            ClassicAssert.AreEqual(FilterValueKind.Number, lit.Value.Kind);
            ClassicAssert.AreEqual(42.0, lit.Value.AsNumber());
        }

        [Test]
        public void Parser_StringLiteral()
        {
            ClassicAssert.IsTrue(VectorFilterTokenizer.TryTokenize("\"hello\"", out var tokens, out _));
            ClassicAssert.IsTrue(VectorFilterParser.TryParseExpression(tokens, 0, out var expr, out _, out _));
            ClassicAssert.IsInstanceOf<LiteralExpr>(expr);
            var lit = (LiteralExpr)expr;
            ClassicAssert.AreEqual(FilterValueKind.String, lit.Value.Kind);
            ClassicAssert.AreEqual("hello", lit.Value.AsString());
        }

        [Test]
        public void Parser_BooleanLiteral()
        {
            ClassicAssert.IsTrue(VectorFilterTokenizer.TryTokenize("true", out var tokens, out _));
            ClassicAssert.IsTrue(VectorFilterParser.TryParseExpression(tokens, 0, out var expr, out _, out _));
            ClassicAssert.IsInstanceOf<LiteralExpr>(expr);
            var lit = (LiteralExpr)expr;
            ClassicAssert.AreEqual(FilterValueKind.Number, lit.Value.Kind);
            ClassicAssert.AreEqual(1.0, lit.Value.AsNumber());

            ClassicAssert.IsTrue(VectorFilterTokenizer.TryTokenize("false", out tokens, out _));
            ClassicAssert.IsTrue(VectorFilterParser.TryParseExpression(tokens, 0, out expr, out _, out _));
            ClassicAssert.IsInstanceOf<LiteralExpr>(expr);
            lit = (LiteralExpr)expr;
            ClassicAssert.AreEqual(FilterValueKind.Number, lit.Value.Kind);
            ClassicAssert.AreEqual(0.0, lit.Value.AsNumber());
        }

        [Test]
        public void Parser_MemberAccess()
        {
            ClassicAssert.IsTrue(VectorFilterTokenizer.TryTokenize(".year", out var tokens, out _));
            ClassicAssert.IsTrue(VectorFilterParser.TryParseExpression(tokens, 0, out var expr, out _, out _));
            ClassicAssert.IsInstanceOf<MemberExpr>(expr);
            ClassicAssert.AreEqual("year", ((MemberExpr)expr).Property);
        }

        [Test]
        public void Parser_UnaryNot()
        {
            ClassicAssert.IsTrue(VectorFilterTokenizer.TryTokenize("not true", out var tokens, out _));
            ClassicAssert.IsTrue(VectorFilterParser.TryParseExpression(tokens, 0, out var expr, out _, out _));
            ClassicAssert.IsInstanceOf<UnaryExpr>(expr);
            var unary = (UnaryExpr)expr;
            ClassicAssert.AreEqual(OperatorKind.Not, unary.Operator);
            ClassicAssert.IsInstanceOf<LiteralExpr>(unary.Operand);
        }

        [Test]
        public void Parser_UnaryNegation()
        {
            ClassicAssert.IsTrue(VectorFilterTokenizer.TryTokenize(".a + (-.b)", out var tokens, out _));
            ClassicAssert.IsTrue(VectorFilterParser.TryParseExpression(tokens, 0, out var expr, out _, out _));
            ClassicAssert.IsInstanceOf<BinaryExpr>(expr);
            var binary = (BinaryExpr)expr;
            ClassicAssert.AreEqual(OperatorKind.Add, binary.Operator);
            ClassicAssert.IsInstanceOf<UnaryExpr>(binary.Right);
            ClassicAssert.AreEqual(OperatorKind.Negate, ((UnaryExpr)binary.Right).Operator);
        }

        [Test]
        public void Parser_OperatorPrecedence_MultiplicationBeforeAddition()
        {
            ClassicAssert.IsTrue(VectorFilterTokenizer.TryTokenize("1 + 2 * 3", out var tokens, out _));
            ClassicAssert.IsTrue(VectorFilterParser.TryParseExpression(tokens, 0, out var expr, out _, out _));
            ClassicAssert.IsInstanceOf<BinaryExpr>(expr);
            var binary = (BinaryExpr)expr;
            ClassicAssert.AreEqual(OperatorKind.Add, binary.Operator);
            ClassicAssert.IsInstanceOf<LiteralExpr>(binary.Left);
            ClassicAssert.IsInstanceOf<BinaryExpr>(binary.Right);
            ClassicAssert.AreEqual(OperatorKind.Multiply, ((BinaryExpr)binary.Right).Operator);
        }

        [Test]
        public void Parser_OperatorPrecedence_AndBeforeOr()
        {
            ClassicAssert.IsTrue(VectorFilterTokenizer.TryTokenize("true or false and true", out var tokens, out _));
            ClassicAssert.IsTrue(VectorFilterParser.TryParseExpression(tokens, 0, out var expr, out _, out _));
            ClassicAssert.IsInstanceOf<BinaryExpr>(expr);
            var binary = (BinaryExpr)expr;
            ClassicAssert.AreEqual(OperatorKind.Or, binary.Operator);
            ClassicAssert.IsInstanceOf<LiteralExpr>(binary.Left);
            ClassicAssert.IsInstanceOf<BinaryExpr>(binary.Right);
            ClassicAssert.AreEqual(OperatorKind.And, ((BinaryExpr)binary.Right).Operator);
        }

        [Test]
        public void Parser_ParenthesesOverridePrecedence()
        {
            ClassicAssert.IsTrue(VectorFilterTokenizer.TryTokenize("(1 + 2) * 3", out var tokens, out _));
            ClassicAssert.IsTrue(VectorFilterParser.TryParseExpression(tokens, 0, out var expr, out _, out _));
            ClassicAssert.IsInstanceOf<BinaryExpr>(expr);
            var binary = (BinaryExpr)expr;
            ClassicAssert.AreEqual(OperatorKind.Multiply, binary.Operator);
            ClassicAssert.IsInstanceOf<BinaryExpr>(binary.Left);
            ClassicAssert.AreEqual(OperatorKind.Add, ((BinaryExpr)binary.Left).Operator);
        }

        [Test]
        public void Parser_Containment()
        {
            ClassicAssert.IsTrue(VectorFilterTokenizer.TryTokenize("\"action\" in .tags", out var tokens, out _));
            ClassicAssert.IsTrue(VectorFilterParser.TryParseExpression(tokens, 0, out var expr, out _, out _));
            ClassicAssert.IsInstanceOf<BinaryExpr>(expr);
            var binary = (BinaryExpr)expr;
            ClassicAssert.AreEqual(OperatorKind.In, binary.Operator);
            ClassicAssert.IsInstanceOf<LiteralExpr>(binary.Left);
            ClassicAssert.IsInstanceOf<MemberExpr>(binary.Right);
        }

        [Test]
        public void Parser_ExponentiationRightAssociative()
        {
            ClassicAssert.IsTrue(VectorFilterTokenizer.TryTokenize("2 ** 3 ** 2", out var tokens, out _));
            ClassicAssert.IsTrue(VectorFilterParser.TryParseExpression(tokens, 0, out var expr, out _, out _));
            ClassicAssert.IsInstanceOf<BinaryExpr>(expr);
            var binary = (BinaryExpr)expr;
            ClassicAssert.AreEqual(OperatorKind.Power, binary.Operator);
            ClassicAssert.IsInstanceOf<LiteralExpr>(binary.Left);
            ClassicAssert.IsInstanceOf<BinaryExpr>(binary.Right);
            ClassicAssert.AreEqual(OperatorKind.Power, ((BinaryExpr)binary.Right).Operator);
        }

        [Test]
        public void Parser_ErrorOnUnexpectedEnd()
        {
            var tokens = new List<Token>();
            ClassicAssert.IsFalse(VectorFilterParser.TryParseExpression(tokens, 0, out var result, out _, out var error));
            ClassicAssert.IsNull(result);
            ClassicAssert.IsNotNull(error);
            ClassicAssert.IsTrue(error.Contains("Unexpected end"));
        }

        [Test]
        public void Parser_ErrorOnMissingClosingParen()
        {
            ClassicAssert.IsTrue(VectorFilterTokenizer.TryTokenize("(1 + 2", out var tokens, out _));
            ClassicAssert.IsFalse(VectorFilterParser.TryParseExpression(tokens, 0, out var result, out _, out var error));
            ClassicAssert.IsNull(result);
            ClassicAssert.IsNotNull(error);
            ClassicAssert.IsTrue(error.Contains("Missing closing parenthesis"));
        }

        [Test]
        public void Parser_ErrorOnInvalidNumberLiteral_DoubleDot()
        {
            // Now caught at tokenization time: "1..023" has multiple decimal points
            ClassicAssert.IsFalse(VectorFilterTokenizer.TryTokenize("1..023", out _, out var error));
            ClassicAssert.IsNotNull(error);
            ClassicAssert.IsTrue(error.Contains("multiple decimal points"));
        }

        [Test]
        public void Parser_ErrorOnInvalidNumberLiteral_MultipleDots()
        {
            // Now caught at tokenization time: "1.2.3" has multiple decimal points
            ClassicAssert.IsFalse(VectorFilterTokenizer.TryTokenize("1.2.3", out _, out var error));
            ClassicAssert.IsNotNull(error);
            ClassicAssert.IsTrue(error.Contains("multiple decimal points"));
        }

        [Test]
        public void Parser_ErrorOnExcessiveRecursionDepth()
        {
            // Build a deeply nested expression: (((((...(1)...))))
            var depth = 100;
            var expr = new string('(', depth) + "1" + new string(')', depth);
            ClassicAssert.IsTrue(VectorFilterTokenizer.TryTokenize(expr, out var tokens, out _));
            ClassicAssert.IsFalse(VectorFilterParser.TryParseExpression(tokens, 0, out var result, out _, out var error));
            ClassicAssert.IsNull(result);
            ClassicAssert.IsNotNull(error);
            ClassicAssert.IsTrue(error.Contains("maximum nesting depth"));
        }

        [Test]
        public void Parser_ErrorOnUnexpectedToken()
        {
            ClassicAssert.IsTrue(VectorFilterTokenizer.TryTokenize(")", out var tokens, out _));
            ClassicAssert.IsFalse(VectorFilterParser.TryParseExpression(tokens, 0, out var result, out _, out var error));
            ClassicAssert.IsNull(result);
            ClassicAssert.IsNotNull(error);
            ClassicAssert.IsTrue(error.Contains("Unexpected token"));
        }
    }
}