// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using Allure.NUnit;
using Garnet.server.Vector.Filter;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    [AllureNUnit]
    [TestFixture]
    public class VectorFilterTests : AllureTestBase
    {
        /// <summary>
        /// Supported vector filter syntax includes:
        /// - Literals: numbers (42, 3.14, -5), strings ("x", 'x'), booleans (true/false)
        /// - Member access: .field, .nested.field, _identifier
        /// - Arithmetic: +, -, *, /, %, ** (power)
        /// - Comparison: &gt;, &lt;, &gt;=, &lt;=, ==, !=
        /// - Logical: and/or/not and aliases &amp;&amp;, ||, !
        /// - Containment: in (for example, "classic" in .tags)
        /// - Grouping: parentheses ( )
        ///
        /// Operator precedence (high to low):
        /// primary/parentheses, unary, power, multiplicative, additive, in,
        /// comparison, equality, and, or.
        /// </summary>

        #region Helper Methods

        /// <summary>
        /// Helper to parse a JSON string into a JsonElement for evaluator tests.
        /// </summary>
        private static JsonElement ParseJson(string json)
        {
            return JsonDocument.Parse(json).RootElement;
        }

        /// <summary>
        /// Helper to tokenize, parse, and evaluate a filter expression against JSON.
        /// Returns object for test backward compatibility.
        /// </summary>
        private static object EvaluateFilter(string expression, string json)
        {
            var tokens = VectorFilterTokenizer.Tokenize(expression);
            var expr = VectorFilterParser.ParseExpression(tokens, 0, out _);
            using var doc = JsonDocument.Parse(json);
            var result = VectorFilterEvaluator.EvaluateExpression(expr, doc.RootElement);

            // Convert FilterValue back to object for test assertions
            return result.Kind switch
            {
                FilterValueKind.Number => (object)result.AsNumber(),
                FilterValueKind.String => result.AsString(),
                FilterValueKind.Null => null,
                _ => result.AsNumber() // fallback
            };
        }

        /// <summary>
        /// Helper to check if a filter expression is truthy against JSON.
        /// </summary>
        private static bool EvaluateFilterTruthy(string expression, string json)
        {
            var tokens = VectorFilterTokenizer.Tokenize(expression);
            var expr = VectorFilterParser.ParseExpression(tokens, 0, out _);
            using var doc = JsonDocument.Parse(json);
            var result = VectorFilterEvaluator.EvaluateExpression(expr, doc.RootElement);
            return VectorFilterEvaluator.IsTruthy(result);
        }

        #endregion

        #region Tokenizer Tests

        [Test]
        public void Tokenizer_IntegerNumbers()
        {
            var tokens = VectorFilterTokenizer.Tokenize("42");
            ClassicAssert.AreEqual(1, tokens.Count);
            ClassicAssert.AreEqual(TokenType.Number, tokens[0].Type);
            ClassicAssert.AreEqual("42", tokens[0].Value);
        }

        [Test]
        public void Tokenizer_DecimalNumbers()
        {
            var tokens = VectorFilterTokenizer.Tokenize("3.14");
            ClassicAssert.AreEqual(1, tokens.Count);
            ClassicAssert.AreEqual(TokenType.Number, tokens[0].Type);
            ClassicAssert.AreEqual("3.14", tokens[0].Value);
        }

        [Test]
        public void Tokenizer_NegativeNumbers()
        {
            var tokens = VectorFilterTokenizer.Tokenize("-5");
            ClassicAssert.AreEqual(1, tokens.Count);
            ClassicAssert.AreEqual(TokenType.Number, tokens[0].Type);
            ClassicAssert.AreEqual("-5", tokens[0].Value);
        }

        [Test]
        public void Tokenizer_StringLiterals()
        {
            var tokens = VectorFilterTokenizer.Tokenize("\"hello\"");
            ClassicAssert.AreEqual(1, tokens.Count);
            ClassicAssert.AreEqual(TokenType.String, tokens[0].Type);
            ClassicAssert.AreEqual("hello", tokens[0].Value);

            tokens = VectorFilterTokenizer.Tokenize("'world'");
            ClassicAssert.AreEqual(1, tokens.Count);
            ClassicAssert.AreEqual(TokenType.String, tokens[0].Type);
            ClassicAssert.AreEqual("world", tokens[0].Value);
        }

        [Test]
        public void Tokenizer_EscapedStringLiterals()
        {
            var tokens = VectorFilterTokenizer.Tokenize("\"hello\\\"world\"");
            ClassicAssert.AreEqual(1, tokens.Count);
            ClassicAssert.AreEqual(TokenType.String, tokens[0].Type);
            ClassicAssert.AreEqual("hello\\\"world", tokens[0].Value);
        }

        [Test]
        public void Tokenizer_UnterminatedStringThrows()
        {
            ClassicAssert.Throws<InvalidOperationException>(() =>
                VectorFilterTokenizer.Tokenize("\"hello"));
        }

        [Test]
        public void Tokenizer_SubtractionNotConfusedWithNegative()
        {
            // ".a - 5" should tokenize as [.a, -, 5], not [.a, -5]
            var tokens = VectorFilterTokenizer.Tokenize(".a - 5");
            ClassicAssert.AreEqual(3, tokens.Count);
            ClassicAssert.AreEqual(TokenType.Identifier, tokens[0].Type);
            ClassicAssert.AreEqual(TokenType.Operator, tokens[1].Type);
            ClassicAssert.AreEqual("-", tokens[1].Value);
            ClassicAssert.AreEqual(TokenType.Number, tokens[2].Type);
            ClassicAssert.AreEqual("5", tokens[2].Value);
        }

        [Test]
        public void Tokenizer_Identifiers()
        {
            var tokens = VectorFilterTokenizer.Tokenize(".year");
            ClassicAssert.AreEqual(1, tokens.Count);
            ClassicAssert.AreEqual(TokenType.Identifier, tokens[0].Type);
            ClassicAssert.AreEqual(".year", tokens[0].Value);

            tokens = VectorFilterTokenizer.Tokenize("_field");
            ClassicAssert.AreEqual(1, tokens.Count);
            ClassicAssert.AreEqual(TokenType.Identifier, tokens[0].Type);
            ClassicAssert.AreEqual("_field", tokens[0].Value);
        }

        [Test]
        public void Tokenizer_Keywords()
        {
            var keywords = new[] { "and", "or", "not", "in" };
            foreach (var kw in keywords)
            {
                var tokens = VectorFilterTokenizer.Tokenize(kw);
                ClassicAssert.AreEqual(1, tokens.Count);
                ClassicAssert.AreEqual(TokenType.Keyword, tokens[0].Type);
                ClassicAssert.AreEqual(kw, tokens[0].Value);
            }
        }

        [Test]
        public void Tokenizer_Booleans()
        {
            var tokens = VectorFilterTokenizer.Tokenize("true");
            ClassicAssert.AreEqual(1, tokens.Count);
            ClassicAssert.AreEqual(TokenType.Boolean, tokens[0].Type);
            ClassicAssert.AreEqual("true", tokens[0].Value);

            tokens = VectorFilterTokenizer.Tokenize("false");
            ClassicAssert.AreEqual(1, tokens.Count);
            ClassicAssert.AreEqual(TokenType.Boolean, tokens[0].Type);
            ClassicAssert.AreEqual("false", tokens[0].Value);
        }

        [Test]
        public void Tokenizer_TwoCharOperators()
        {
            var ops = new[] { "==", "!=", ">=", "<=", "&&", "||", "**" };
            foreach (var op in ops)
            {
                var tokens = VectorFilterTokenizer.Tokenize($"1 {op} 2");
                var opToken = tokens.First(t => t.Type == TokenType.Operator);
                ClassicAssert.AreEqual(op, opToken.Value);
            }
        }

        [Test]
        public void Tokenizer_SingleCharOperators()
        {
            var ops = new[] { ">", "<", "+", "-", "*", "/", "%", "!" };
            foreach (var op in ops)
            {
                // Use identifiers to avoid ambiguity with negative numbers for "-"
                var tokens = VectorFilterTokenizer.Tokenize($".a {op} .b");
                var opToken = tokens.First(t => t.Type == TokenType.Operator);
                ClassicAssert.AreEqual(op, opToken.Value);
            }
        }

        [Test]
        public void Tokenizer_Delimiters()
        {
            var tokens = VectorFilterTokenizer.Tokenize("(.year > 10)");
            ClassicAssert.AreEqual(TokenType.Delimiter, tokens[0].Type);
            ClassicAssert.AreEqual("(", tokens[0].Value);
            ClassicAssert.AreEqual(TokenType.Delimiter, tokens[4].Type);
            ClassicAssert.AreEqual(")", tokens[4].Value);
        }

        [Test]
        public void Tokenizer_ComplexExpression()
        {
            var tokens = VectorFilterTokenizer.Tokenize(".year > 1950 and .rating >= 4.0");
            ClassicAssert.AreEqual(7, tokens.Count);
            ClassicAssert.AreEqual(TokenType.Identifier, tokens[0].Type);  // .year
            ClassicAssert.AreEqual(TokenType.Operator, tokens[1].Type);    // >
            ClassicAssert.AreEqual(TokenType.Number, tokens[2].Type);      // 1950
            ClassicAssert.AreEqual(TokenType.Keyword, tokens[3].Type);     // and
            ClassicAssert.AreEqual(TokenType.Identifier, tokens[4].Type);  // .rating
            ClassicAssert.AreEqual(TokenType.Operator, tokens[5].Type);    // >=
            ClassicAssert.AreEqual(TokenType.Number, tokens[6].Type);      // 4.0
        }

        [Test]
        public void Tokenizer_EmptyInput()
        {
            var tokens = VectorFilterTokenizer.Tokenize("");
            ClassicAssert.AreEqual(0, tokens.Count);

            tokens = VectorFilterTokenizer.Tokenize("   ");
            ClassicAssert.AreEqual(0, tokens.Count);
        }

        #endregion

        #region Parser Tests

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
            // 1 + 2 * 3 should parse as 1 + (2 * 3)
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
            // a or b and c should parse as a or (b and c)
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
            // (1 + 2) * 3 should parse as (1 + 2) * 3
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
            // 2 ** 3 ** 2 should parse as 2 ** (3 ** 2)
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

        #endregion

        #region Evaluator Tests

        [Test]
        public void Evaluator_Arithmetic()
        {
            var json = "{}";
            ClassicAssert.AreEqual(5.0, EvaluateFilter("2 + 3", json));
            ClassicAssert.AreEqual(1.0, EvaluateFilter("3 - 2", json));
            ClassicAssert.AreEqual(6.0, EvaluateFilter("2 * 3", json));
            ClassicAssert.AreEqual(2.5, EvaluateFilter("5 / 2", json));
            ClassicAssert.AreEqual(1.0, EvaluateFilter("7 % 3", json));
            ClassicAssert.AreEqual(8.0, EvaluateFilter("2 ** 3", json));
        }

        [Test]
        public void Evaluator_SubtractionWithField()
        {
            var json = "{\"year\":1980}";
            ClassicAssert.AreEqual(1975.0, EvaluateFilter(".year - 5", json));
            ClassicAssert.IsTrue(EvaluateFilterTruthy(".year - 5 > 0", json));
        }

        [Test]
        public void Evaluator_Comparison()
        {
            var json = "{}";
            ClassicAssert.AreEqual(1.0, EvaluateFilter("5 > 3", json));
            ClassicAssert.AreEqual(0.0, EvaluateFilter("3 > 5", json));
            ClassicAssert.AreEqual(1.0, EvaluateFilter("3 < 5", json));
            ClassicAssert.AreEqual(0.0, EvaluateFilter("5 < 3", json));
            ClassicAssert.AreEqual(1.0, EvaluateFilter("5 >= 5", json));
            ClassicAssert.AreEqual(1.0, EvaluateFilter("5 <= 5", json));
            ClassicAssert.AreEqual(1.0, EvaluateFilter("5 == 5", json));
            ClassicAssert.AreEqual(1.0, EvaluateFilter("5 != 3", json));
            ClassicAssert.AreEqual(0.0, EvaluateFilter("5 != 5", json));
        }

        [Test]
        public void Evaluator_LogicalAnd()
        {
            var json = "{}";
            ClassicAssert.IsTrue(EvaluateFilterTruthy("true and true", json));
            ClassicAssert.IsFalse(EvaluateFilterTruthy("true and false", json));
            ClassicAssert.IsFalse(EvaluateFilterTruthy("false and true", json));
            // Also test && syntax
            ClassicAssert.IsTrue(EvaluateFilterTruthy("true && true", json));
        }

        [Test]
        public void Evaluator_LogicalOr()
        {
            var json = "{}";
            ClassicAssert.IsTrue(EvaluateFilterTruthy("true or false", json));
            ClassicAssert.IsTrue(EvaluateFilterTruthy("false or true", json));
            ClassicAssert.IsFalse(EvaluateFilterTruthy("false or false", json));
            // Also test || syntax
            ClassicAssert.IsTrue(EvaluateFilterTruthy("false || true", json));
        }

        [Test]
        public void Evaluator_LogicalNot()
        {
            var json = "{}";
            ClassicAssert.IsFalse(EvaluateFilterTruthy("not true", json));
            ClassicAssert.IsTrue(EvaluateFilterTruthy("not false", json));
        }

        [Test]
        public void Evaluator_StringEquality()
        {
            var json = "{\"genre\":\"action\"}";
            ClassicAssert.IsTrue(EvaluateFilterTruthy(".genre == \"action\"", json));
            ClassicAssert.IsFalse(EvaluateFilterTruthy(".genre == \"drama\"", json));
            ClassicAssert.IsTrue(EvaluateFilterTruthy(".genre != \"drama\"", json));
        }

        [Test]
        public void Evaluator_MemberAccess()
        {
            var json = "{\"year\":1980,\"rating\":4.5}";
            ClassicAssert.AreEqual(1980.0, EvaluateFilter(".year", json));
            ClassicAssert.AreEqual(4.5, EvaluateFilter(".rating", json));
        }

        [Test]
        public void Evaluator_MissingFieldReturnsNull()
        {
            var json = "{\"year\":1980}";
            var result = EvaluateFilter(".missing", json);
            ClassicAssert.IsNull(result);
            ClassicAssert.IsFalse(EvaluateFilterTruthy(".missing", json));
        }

        [Test]
        public void Evaluator_InOperatorWithArray()
        {
            var json = "{\"tags\":[\"classic\",\"popular\"]}";
            ClassicAssert.IsTrue(EvaluateFilterTruthy("\"classic\" in .tags", json));
            ClassicAssert.IsTrue(EvaluateFilterTruthy("\"popular\" in .tags", json));
            ClassicAssert.IsFalse(EvaluateFilterTruthy("\"modern\" in .tags", json));
        }

        [Test]
        public void Evaluator_InOperatorWithNumericArray()
        {
            var json = "{\"scores\":[1,2,3]}";
            ClassicAssert.IsTrue(EvaluateFilterTruthy("2 in .scores", json));
            ClassicAssert.IsFalse(EvaluateFilterTruthy("5 in .scores", json));
        }

        [Test]
        public void Evaluator_IsTruthy()
        {
            // Test the object-accepting overload for backward compatibility
            ClassicAssert.IsFalse(VectorFilterEvaluator.IsTruthy(null));
            ClassicAssert.IsFalse(VectorFilterEvaluator.IsTruthy(0.0));
            ClassicAssert.IsFalse(VectorFilterEvaluator.IsTruthy(0));
            ClassicAssert.IsFalse(VectorFilterEvaluator.IsTruthy(""));
            ClassicAssert.IsFalse(VectorFilterEvaluator.IsTruthy(false));

            ClassicAssert.IsTrue(VectorFilterEvaluator.IsTruthy(1.0));
            ClassicAssert.IsTrue(VectorFilterEvaluator.IsTruthy(-1.0));
            ClassicAssert.IsTrue(VectorFilterEvaluator.IsTruthy(42));
            ClassicAssert.IsTrue(VectorFilterEvaluator.IsTruthy("hello"));
            ClassicAssert.IsTrue(VectorFilterEvaluator.IsTruthy(true));
        }

        [Test]
        public void Evaluator_IsTruthy_FilterValue()
        {
            // Test the FilterValue-accepting overload (the hot-path version)
            ClassicAssert.IsFalse(VectorFilterEvaluator.IsTruthy(FilterValue.Null));
            ClassicAssert.IsFalse(VectorFilterEvaluator.IsTruthy(FilterValue.False));
            ClassicAssert.IsFalse(VectorFilterEvaluator.IsTruthy(FilterValue.FromNumber(0.0)));
            ClassicAssert.IsFalse(VectorFilterEvaluator.IsTruthy(FilterValue.FromString("")));

            ClassicAssert.IsTrue(VectorFilterEvaluator.IsTruthy(FilterValue.True));
            ClassicAssert.IsTrue(VectorFilterEvaluator.IsTruthy(FilterValue.FromNumber(1.0)));
            ClassicAssert.IsTrue(VectorFilterEvaluator.IsTruthy(FilterValue.FromNumber(-1.0)));
            ClassicAssert.IsTrue(VectorFilterEvaluator.IsTruthy(FilterValue.FromString("hello")));
        }

        [Test]
        public void Evaluator_ComplexExpression()
        {
            var json = "{\"year\":1980,\"rating\":4.5,\"genre\":\"action\",\"tags\":[\"classic\",\"popular\"]}";

            // .rating * 2 > 8 and (.year >= 1980 or "modern" in .tags)
            ClassicAssert.IsTrue(EvaluateFilterTruthy(
                ".rating * 2 > 8 and (.year >= 1980 or \"modern\" in .tags)", json));

            // (.year > 2000 or .year < 1970) and .rating >= 4.0
            ClassicAssert.IsFalse(EvaluateFilterTruthy(
                "(.year > 2000 or .year < 1970) and .rating >= 4.0", json));

            // not (.genre == "drama")
            ClassicAssert.IsTrue(EvaluateFilterTruthy("not (.genre == \"drama\")", json));

            // .year / 10 >= 198
            ClassicAssert.IsTrue(EvaluateFilterTruthy(".year / 10 >= 198", json));
        }

        [Test]
        public void Evaluator_ComparisonWithMissingField()
        {
            var json = "{\"year\":1980}";
            // Missing field compared to number: ToNumber(null) = 0, so 0 > 1950 is false
            ClassicAssert.IsFalse(EvaluateFilterTruthy(".missing > 1950", json));
        }

        [Test]
        public void Evaluator_BooleanJsonValues()
        {
            var json = "{\"active\":true,\"deleted\":false}";
            ClassicAssert.IsTrue(EvaluateFilterTruthy(".active", json));
            ClassicAssert.IsFalse(EvaluateFilterTruthy(".deleted", json));
            ClassicAssert.IsTrue(EvaluateFilterTruthy(".active == true", json));
        }

        #endregion
    }
}