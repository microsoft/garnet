// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
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
            var tokens = VectorFilterTokenizer.Tokenize("42");
            var expr = VectorFilterParser.ParseExpression(tokens, 0, out var end);
            ClassicAssert.AreEqual(1, end);
            ClassicAssert.IsInstanceOf<LiteralExpr>(expr);
            var lit = (LiteralExpr)expr;
            ClassicAssert.AreEqual(FilterValueKind.Number, lit.Value.Kind);
            ClassicAssert.AreEqual(42.0, lit.Value.AsNumber());
        }

        [Test]
        public void Parser_StringLiteral()
        {
            var tokens = VectorFilterTokenizer.Tokenize("\"hello\"");
            var expr = VectorFilterParser.ParseExpression(tokens, 0, out _);
            ClassicAssert.IsInstanceOf<LiteralExpr>(expr);
            var lit = (LiteralExpr)expr;
            ClassicAssert.AreEqual(FilterValueKind.String, lit.Value.Kind);
            ClassicAssert.AreEqual("hello", lit.Value.AsString());
        }

        [Test]
        public void Parser_BooleanLiteral()
        {
            var tokens = VectorFilterTokenizer.Tokenize("true");
            var expr = VectorFilterParser.ParseExpression(tokens, 0, out _);
            ClassicAssert.IsInstanceOf<LiteralExpr>(expr);
            var lit = (LiteralExpr)expr;
            ClassicAssert.AreEqual(FilterValueKind.Number, lit.Value.Kind);
            ClassicAssert.AreEqual(1.0, lit.Value.AsNumber());

            tokens = VectorFilterTokenizer.Tokenize("false");
            expr = VectorFilterParser.ParseExpression(tokens, 0, out _);
            ClassicAssert.IsInstanceOf<LiteralExpr>(expr);
            lit = (LiteralExpr)expr;
            ClassicAssert.AreEqual(FilterValueKind.Number, lit.Value.Kind);
            ClassicAssert.AreEqual(0.0, lit.Value.AsNumber());
        }

        [Test]
        public void Parser_MemberAccess()
        {
            var tokens = VectorFilterTokenizer.Tokenize(".year");
            var expr = VectorFilterParser.ParseExpression(tokens, 0, out _);
            ClassicAssert.IsInstanceOf<MemberExpr>(expr);
            ClassicAssert.AreEqual("year", ((MemberExpr)expr).Property);
        }

        [Test]
        public void Parser_UnaryNot()
        {
            var tokens = VectorFilterTokenizer.Tokenize("not true");
            var expr = VectorFilterParser.ParseExpression(tokens, 0, out _);
            ClassicAssert.IsInstanceOf<UnaryExpr>(expr);
            var unary = (UnaryExpr)expr;
            ClassicAssert.AreEqual(OperatorKind.Not, unary.Operator);
            ClassicAssert.IsInstanceOf<LiteralExpr>(unary.Operand);
        }

        [Test]
        public void Parser_UnaryNegation()
        {
            var tokens = VectorFilterTokenizer.Tokenize(".a + (-.b)");
            var expr = VectorFilterParser.ParseExpression(tokens, 0, out _);
            ClassicAssert.IsInstanceOf<BinaryExpr>(expr);
            var binary = (BinaryExpr)expr;
            ClassicAssert.AreEqual(OperatorKind.Add, binary.Operator);
            ClassicAssert.IsInstanceOf<UnaryExpr>(binary.Right);
            ClassicAssert.AreEqual(OperatorKind.Negate, ((UnaryExpr)binary.Right).Operator);
        }

        [Test]
        public void Parser_OperatorPrecedence_MultiplicationBeforeAddition()
        {
            var tokens = VectorFilterTokenizer.Tokenize("1 + 2 * 3");
            var expr = VectorFilterParser.ParseExpression(tokens, 0, out _);
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
            var tokens = VectorFilterTokenizer.Tokenize("true or false and true");
            var expr = VectorFilterParser.ParseExpression(tokens, 0, out _);
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
            var tokens = VectorFilterTokenizer.Tokenize("(1 + 2) * 3");
            var expr = VectorFilterParser.ParseExpression(tokens, 0, out _);
            ClassicAssert.IsInstanceOf<BinaryExpr>(expr);
            var binary = (BinaryExpr)expr;
            ClassicAssert.AreEqual(OperatorKind.Multiply, binary.Operator);
            ClassicAssert.IsInstanceOf<BinaryExpr>(binary.Left);
            ClassicAssert.AreEqual(OperatorKind.Add, ((BinaryExpr)binary.Left).Operator);
        }

        [Test]
        public void Parser_Containment()
        {
            var tokens = VectorFilterTokenizer.Tokenize("\"action\" in .tags");
            var expr = VectorFilterParser.ParseExpression(tokens, 0, out _);
            ClassicAssert.IsInstanceOf<BinaryExpr>(expr);
            var binary = (BinaryExpr)expr;
            ClassicAssert.AreEqual(OperatorKind.In, binary.Operator);
            ClassicAssert.IsInstanceOf<LiteralExpr>(binary.Left);
            ClassicAssert.IsInstanceOf<MemberExpr>(binary.Right);
        }

        [Test]
        public void Parser_ExponentiationRightAssociative()
        {
            var tokens = VectorFilterTokenizer.Tokenize("2 ** 3 ** 2");
            var expr = VectorFilterParser.ParseExpression(tokens, 0, out _);
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
            ClassicAssert.Throws<InvalidOperationException>(() =>
                VectorFilterParser.ParseExpression(tokens, 0, out _));
        }

        [Test]
        public void Parser_ErrorOnMissingClosingParen()
        {
            var tokens = VectorFilterTokenizer.Tokenize("(1 + 2");
            ClassicAssert.Throws<InvalidOperationException>(() =>
                VectorFilterParser.ParseExpression(tokens, 0, out _));
        }

        [Test]
        public void Parser_ErrorOnInvalidNumberLiteral_DoubleDot()
        {
            var tokens = VectorFilterTokenizer.Tokenize("1..023");
            ClassicAssert.Throws<FormatException>(() =>
                VectorFilterParser.ParseExpression(tokens, 0, out _));
        }

        [Test]
        public void Parser_ErrorOnInvalidNumberLiteral_MultipleDots()
        {
            var tokens = VectorFilterTokenizer.Tokenize("1.2.3");
            ClassicAssert.Throws<FormatException>(() =>
                VectorFilterParser.ParseExpression(tokens, 0, out _));
        }
    }
}