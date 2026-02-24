// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using Allure.NUnit;
using Garnet.server.Vector.Filter;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    [AllureNUnit]
    [TestFixture]
    public class VectorFilterTokenizerTests : AllureTestBase
    {
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
            ClassicAssert.AreEqual(TokenType.Identifier, tokens[0].Type);
            ClassicAssert.AreEqual(TokenType.Operator, tokens[1].Type);
            ClassicAssert.AreEqual(TokenType.Number, tokens[2].Type);
            ClassicAssert.AreEqual(TokenType.Keyword, tokens[3].Type);
            ClassicAssert.AreEqual(TokenType.Identifier, tokens[4].Type);
            ClassicAssert.AreEqual(TokenType.Operator, tokens[5].Type);
            ClassicAssert.AreEqual(TokenType.Number, tokens[6].Type);
        }

        [Test]
        public void Tokenizer_EmptyInput()
        {
            var tokens = VectorFilterTokenizer.Tokenize("");
            ClassicAssert.AreEqual(0, tokens.Count);

            tokens = VectorFilterTokenizer.Tokenize("   ");
            ClassicAssert.AreEqual(0, tokens.Count);
        }
    }
}