// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

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
            ClassicAssert.IsTrue(VectorFilterTokenizer.TryTokenize("42", out var tokens, out _));
            ClassicAssert.AreEqual(1, tokens.Count);
            ClassicAssert.AreEqual(TokenType.Number, tokens[0].Type);
            ClassicAssert.AreEqual("42", tokens[0].Value);
        }

        [Test]
        public void Tokenizer_DecimalNumbers()
        {
            ClassicAssert.IsTrue(VectorFilterTokenizer.TryTokenize("3.14", out var tokens, out _));
            ClassicAssert.AreEqual(1, tokens.Count);
            ClassicAssert.AreEqual(TokenType.Number, tokens[0].Type);
            ClassicAssert.AreEqual("3.14", tokens[0].Value);
        }

        [Test]
        public void Tokenizer_NegativeNumbers()
        {
            ClassicAssert.IsTrue(VectorFilterTokenizer.TryTokenize("-5", out var tokens, out _));
            ClassicAssert.AreEqual(1, tokens.Count);
            ClassicAssert.AreEqual(TokenType.Number, tokens[0].Type);
            ClassicAssert.AreEqual("-5", tokens[0].Value);
        }

        [Test]
        public void Tokenizer_StringLiterals()
        {
            ClassicAssert.IsTrue(VectorFilterTokenizer.TryTokenize("\"hello\"", out var tokens, out _));
            ClassicAssert.AreEqual(1, tokens.Count);
            ClassicAssert.AreEqual(TokenType.String, tokens[0].Type);
            ClassicAssert.AreEqual("hello", tokens[0].Value);

            ClassicAssert.IsTrue(VectorFilterTokenizer.TryTokenize("'world'", out tokens, out _));
            ClassicAssert.AreEqual(1, tokens.Count);
            ClassicAssert.AreEqual(TokenType.String, tokens[0].Type);
            ClassicAssert.AreEqual("world", tokens[0].Value);
        }

        [Test]
        public void Tokenizer_EscapedStringLiterals()
        {
            ClassicAssert.IsTrue(VectorFilterTokenizer.TryTokenize("\"hello\\\"world\"", out var tokens, out _));
            ClassicAssert.AreEqual(1, tokens.Count);
            ClassicAssert.AreEqual(TokenType.String, tokens[0].Type);
            ClassicAssert.AreEqual("hello\\\"world", tokens[0].Value);
        }

        [Test]
        public void Tokenizer_UnterminatedStringReturnsFalse()
        {
            ClassicAssert.IsFalse(VectorFilterTokenizer.TryTokenize("\"hello", out var tokens, out var error));
            ClassicAssert.IsNull(tokens);
            ClassicAssert.IsNotNull(error);
            ClassicAssert.IsTrue(error.Contains("Unterminated string"));
        }

        [Test]
        public void Tokenizer_SubtractionNotConfusedWithNegative()
        {
            ClassicAssert.IsTrue(VectorFilterTokenizer.TryTokenize(".a - 5", out var tokens, out _));
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
            ClassicAssert.IsTrue(VectorFilterTokenizer.TryTokenize(".year", out var tokens, out _));
            ClassicAssert.AreEqual(1, tokens.Count);
            ClassicAssert.AreEqual(TokenType.Identifier, tokens[0].Type);
            ClassicAssert.AreEqual(".year", tokens[0].Value);

            ClassicAssert.IsTrue(VectorFilterTokenizer.TryTokenize("_field", out tokens, out _));
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
                ClassicAssert.IsTrue(VectorFilterTokenizer.TryTokenize(kw, out var tokens, out _));
                ClassicAssert.AreEqual(1, tokens.Count);
                ClassicAssert.AreEqual(TokenType.Keyword, tokens[0].Type);
                ClassicAssert.AreEqual(kw, tokens[0].Value);
            }
        }

        [Test]
        public void Tokenizer_Booleans()
        {
            ClassicAssert.IsTrue(VectorFilterTokenizer.TryTokenize("true", out var tokens, out _));
            ClassicAssert.AreEqual(1, tokens.Count);
            ClassicAssert.AreEqual(TokenType.Boolean, tokens[0].Type);
            ClassicAssert.AreEqual("true", tokens[0].Value);

            ClassicAssert.IsTrue(VectorFilterTokenizer.TryTokenize("false", out tokens, out _));
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
                ClassicAssert.IsTrue(VectorFilterTokenizer.TryTokenize($"1 {op} 2", out var tokens, out _));
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
                ClassicAssert.IsTrue(VectorFilterTokenizer.TryTokenize($".a {op} .b", out var tokens, out _));
                var opToken = tokens.First(t => t.Type == TokenType.Operator);
                ClassicAssert.AreEqual(op, opToken.Value);
            }
        }

        [Test]
        public void Tokenizer_Delimiters()
        {
            ClassicAssert.IsTrue(VectorFilterTokenizer.TryTokenize("(.year > 10)", out var tokens, out _));
            ClassicAssert.AreEqual(TokenType.Delimiter, tokens[0].Type);
            ClassicAssert.AreEqual("(", tokens[0].Value);
            ClassicAssert.AreEqual(TokenType.Delimiter, tokens[4].Type);
            ClassicAssert.AreEqual(")", tokens[4].Value);
        }

        [Test]
        public void Tokenizer_ComplexExpression()
        {
            ClassicAssert.IsTrue(VectorFilterTokenizer.TryTokenize(".year > 1950 and .rating >= 4.0", out var tokens, out _));
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
            ClassicAssert.IsTrue(VectorFilterTokenizer.TryTokenize("", out var tokens, out _));
            ClassicAssert.AreEqual(0, tokens.Count);

            ClassicAssert.IsTrue(VectorFilterTokenizer.TryTokenize("   ", out tokens, out _));
            ClassicAssert.AreEqual(0, tokens.Count);
        }

        [Test]
        public void Tokenizer_UnexpectedCharacterReturnsFalse()
        {
            ClassicAssert.IsFalse(VectorFilterTokenizer.TryTokenize("@", out var tokens, out var error));
            ClassicAssert.IsNull(tokens);
            ClassicAssert.IsNotNull(error);
            ClassicAssert.IsTrue(error.Contains("Unexpected character"));
        }

        [Test]
        public void Tokenizer_MultipleDotsInNumberReturnsFalse()
        {
            ClassicAssert.IsFalse(VectorFilterTokenizer.TryTokenize("1.2.3", out var tokens, out var error));
            ClassicAssert.IsNull(tokens);
            ClassicAssert.IsNotNull(error);
            ClassicAssert.IsTrue(error.Contains("multiple decimal points"));
        }

        [Test]
        public void Tokenizer_DoubleDotInNumberReturnsFalse()
        {
            ClassicAssert.IsFalse(VectorFilterTokenizer.TryTokenize("1..023", out var tokens, out var error));
            ClassicAssert.IsNull(tokens);
            ClassicAssert.IsNotNull(error);
            ClassicAssert.IsTrue(error.Contains("multiple decimal points"));
        }

        [Test]
        public void Tokenizer_ManyDotsInNumberReturnsFalse()
        {
            ClassicAssert.IsFalse(VectorFilterTokenizer.TryTokenize("123.....23", out var tokens, out var error));
            ClassicAssert.IsNull(tokens);
            ClassicAssert.IsNotNull(error);
            ClassicAssert.IsTrue(error.Contains("multiple decimal points"));
        }
    }
}
