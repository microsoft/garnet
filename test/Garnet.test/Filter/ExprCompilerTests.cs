// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Text;
using Allure.NUnit;
using Garnet.server.Vector.Filter;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    /// <summary>
    /// Tests for the ExprCompiler (shunting-yard tokenizer + compiler).
    /// Verifies tokenization and compilation to flat postfix programs.
    /// </summary>
    [AllureNUnit]
    [TestFixture]
    public class ExprCompilerTests : AllureTestBase
    {
        [Test]
        public void Compiler_IntegerNumbers()
        {
            var program = ExprCompiler.TryCompile(Encoding.UTF8.GetBytes("42"), out _);
            ClassicAssert.IsNotNull(program);
            ClassicAssert.AreEqual(1, program.Length);
            ClassicAssert.AreEqual(ExprTokenType.Num, program.Instructions[0].TokenType);
            ClassicAssert.AreEqual(42.0, program.Instructions[0].Num);
        }

        [Test]
        public void Compiler_DecimalNumbers()
        {
            var program = ExprCompiler.TryCompile(Encoding.UTF8.GetBytes("3.14"), out _);
            ClassicAssert.IsNotNull(program);
            ClassicAssert.AreEqual(1, program.Length);
            ClassicAssert.AreEqual(ExprTokenType.Num, program.Instructions[0].TokenType);
            ClassicAssert.AreEqual(3.14, program.Instructions[0].Num, 0.001);
        }

        [Test]
        public void Compiler_NegativeNumbers()
        {
            var program = ExprCompiler.TryCompile(Encoding.UTF8.GetBytes("-5"), out _);
            ClassicAssert.IsNotNull(program);
            ClassicAssert.AreEqual(1, program.Length);
            ClassicAssert.AreEqual(ExprTokenType.Num, program.Instructions[0].TokenType);
            ClassicAssert.AreEqual(-5.0, program.Instructions[0].Num);
        }

        [Test]
        public void Compiler_StringLiterals()
        {
            var program = ExprCompiler.TryCompile(Encoding.UTF8.GetBytes("\"hello\""), out _);
            ClassicAssert.IsNotNull(program);
            ClassicAssert.AreEqual(1, program.Length);
            ClassicAssert.AreEqual(ExprTokenType.Str, program.Instructions[0].TokenType);
            ClassicAssert.AreEqual("hello", program.Instructions[0].Str);

            program = ExprCompiler.TryCompile(Encoding.UTF8.GetBytes("'world'"), out _);
            ClassicAssert.IsNotNull(program);
            ClassicAssert.AreEqual(1, program.Length);
            ClassicAssert.AreEqual(ExprTokenType.Str, program.Instructions[0].TokenType);
            ClassicAssert.AreEqual("world", program.Instructions[0].Str);
        }

        [Test]
        public void Compiler_EscapedStringLiterals()
        {
            var program = ExprCompiler.TryCompile(Encoding.UTF8.GetBytes("\"hello\\\"world\""), out _);
            ClassicAssert.IsNotNull(program);
            ClassicAssert.AreEqual(1, program.Length);
            ClassicAssert.AreEqual(ExprTokenType.Str, program.Instructions[0].TokenType);
            ClassicAssert.AreEqual("hello\"world", program.Instructions[0].Str);
        }

        [Test]
        public void Compiler_UnterminatedStringReturnsFalse()
        {
            var program = ExprCompiler.TryCompile(Encoding.UTF8.GetBytes("\"hello"), out _);
            ClassicAssert.IsNull(program);
        }

        [Test]
        public void Compiler_SubtractionNotConfusedWithNegative()
        {
            // ".a - 5" → postfix: [SEL:a] [NUM:5] [OP:Sub]
            var program = ExprCompiler.TryCompile(Encoding.UTF8.GetBytes(".a - 5"), out _);
            ClassicAssert.IsNotNull(program);
            ClassicAssert.AreEqual(3, program.Length);
            ClassicAssert.AreEqual(ExprTokenType.Selector, program.Instructions[0].TokenType);
            ClassicAssert.AreEqual(ExprTokenType.Num, program.Instructions[1].TokenType);
            ClassicAssert.AreEqual(5.0, program.Instructions[1].Num);
            ClassicAssert.AreEqual(ExprTokenType.Op, program.Instructions[2].TokenType);
            ClassicAssert.AreEqual(OpCode.Sub, program.Instructions[2].OpCode);
        }

        [Test]
        public void Compiler_Selectors()
        {
            var program = ExprCompiler.TryCompile(Encoding.UTF8.GetBytes(".year"), out _);
            ClassicAssert.IsNotNull(program);
            ClassicAssert.AreEqual(1, program.Length);
            ClassicAssert.AreEqual(ExprTokenType.Selector, program.Instructions[0].TokenType);
            ClassicAssert.AreEqual("year", program.Instructions[0].Str);
        }

        [Test]
        public void Compiler_Keywords()
        {
            // "true and false" → [NUM:1] [NUM:0] [OP:And]
            var program = ExprCompiler.TryCompile(Encoding.UTF8.GetBytes("true and false"), out _);
            ClassicAssert.IsNotNull(program);
            ClassicAssert.AreEqual(3, program.Length);
            ClassicAssert.AreEqual(ExprTokenType.Num, program.Instructions[0].TokenType);
            ClassicAssert.AreEqual(1.0, program.Instructions[0].Num);
            ClassicAssert.AreEqual(ExprTokenType.Num, program.Instructions[1].TokenType);
            ClassicAssert.AreEqual(0.0, program.Instructions[1].Num);
            ClassicAssert.AreEqual(ExprTokenType.Op, program.Instructions[2].TokenType);
            ClassicAssert.AreEqual(OpCode.And, program.Instructions[2].OpCode);
        }

        [Test]
        public void Compiler_Booleans()
        {
            var program = ExprCompiler.TryCompile(Encoding.UTF8.GetBytes("true"), out _);
            ClassicAssert.IsNotNull(program);
            ClassicAssert.AreEqual(1, program.Length);
            ClassicAssert.AreEqual(ExprTokenType.Num, program.Instructions[0].TokenType);
            ClassicAssert.AreEqual(1.0, program.Instructions[0].Num);

            program = ExprCompiler.TryCompile(Encoding.UTF8.GetBytes("false"), out _);
            ClassicAssert.IsNotNull(program);
            ClassicAssert.AreEqual(1, program.Length);
            ClassicAssert.AreEqual(ExprTokenType.Num, program.Instructions[0].TokenType);
            ClassicAssert.AreEqual(0.0, program.Instructions[0].Num);
        }

        [Test]
        public void Compiler_TwoCharOperators()
        {
            var program = ExprCompiler.TryCompile(Encoding.UTF8.GetBytes("1 == 2"), out _);
            ClassicAssert.IsNotNull(program);
            ClassicAssert.AreEqual(OpCode.Eq, program.Instructions[2].OpCode);

            program = ExprCompiler.TryCompile(Encoding.UTF8.GetBytes("1 != 2"), out _);
            ClassicAssert.IsNotNull(program);
            ClassicAssert.AreEqual(OpCode.Neq, program.Instructions[2].OpCode);

            program = ExprCompiler.TryCompile(Encoding.UTF8.GetBytes("1 >= 2"), out _);
            ClassicAssert.IsNotNull(program);
            ClassicAssert.AreEqual(OpCode.Gte, program.Instructions[2].OpCode);

            program = ExprCompiler.TryCompile(Encoding.UTF8.GetBytes("1 <= 2"), out _);
            ClassicAssert.IsNotNull(program);
            ClassicAssert.AreEqual(OpCode.Lte, program.Instructions[2].OpCode);

            program = ExprCompiler.TryCompile(Encoding.UTF8.GetBytes("true && false"), out _);
            ClassicAssert.IsNotNull(program);
            ClassicAssert.AreEqual(OpCode.And, program.Instructions[2].OpCode);

            program = ExprCompiler.TryCompile(Encoding.UTF8.GetBytes("true || false"), out _);
            ClassicAssert.IsNotNull(program);
            ClassicAssert.AreEqual(OpCode.Or, program.Instructions[2].OpCode);

            program = ExprCompiler.TryCompile(Encoding.UTF8.GetBytes("2 ** 3"), out _);
            ClassicAssert.IsNotNull(program);
            ClassicAssert.AreEqual(OpCode.Pow, program.Instructions[2].OpCode);
        }

        [Test]
        public void Compiler_SingleCharOperators()
        {
            var program = ExprCompiler.TryCompile(Encoding.UTF8.GetBytes("1 > 2"), out _);
            ClassicAssert.IsNotNull(program);
            ClassicAssert.AreEqual(OpCode.Gt, program.Instructions[2].OpCode);

            program = ExprCompiler.TryCompile(Encoding.UTF8.GetBytes("1 < 2"), out _);
            ClassicAssert.IsNotNull(program);
            ClassicAssert.AreEqual(OpCode.Lt, program.Instructions[2].OpCode);

            program = ExprCompiler.TryCompile(Encoding.UTF8.GetBytes("1 + 2"), out _);
            ClassicAssert.IsNotNull(program);
            ClassicAssert.AreEqual(OpCode.Add, program.Instructions[2].OpCode);

            program = ExprCompiler.TryCompile(Encoding.UTF8.GetBytes("1 * 2"), out _);
            ClassicAssert.IsNotNull(program);
            ClassicAssert.AreEqual(OpCode.Mul, program.Instructions[2].OpCode);

            program = ExprCompiler.TryCompile(Encoding.UTF8.GetBytes("1 / 2"), out _);
            ClassicAssert.IsNotNull(program);
            ClassicAssert.AreEqual(OpCode.Div, program.Instructions[2].OpCode);

            program = ExprCompiler.TryCompile(Encoding.UTF8.GetBytes("1 % 2"), out _);
            ClassicAssert.IsNotNull(program);
            ClassicAssert.AreEqual(OpCode.Mod, program.Instructions[2].OpCode);
        }

        [Test]
        public void Compiler_Parentheses()
        {
            var program = ExprCompiler.TryCompile(Encoding.UTF8.GetBytes("(.year > 10)"), out _);
            ClassicAssert.IsNotNull(program);
            // Postfix: [SEL:year] [NUM:10] [OP:Gt]
            ClassicAssert.AreEqual(3, program.Length);
        }

        [Test]
        public void Compiler_ComplexExpression()
        {
            var program = ExprCompiler.TryCompile(Encoding.UTF8.GetBytes(".year > 1950 and .rating >= 4.0"), out _);
            ClassicAssert.IsNotNull(program);
            // Postfix: [SEL:year] [NUM:1950] [OP:Gt] [SEL:rating] [NUM:4.0] [OP:Gte] [OP:And]
            ClassicAssert.AreEqual(7, program.Length);
        }

        [Test]
        public void Compiler_EmptyInput()
        {
            var program = ExprCompiler.TryCompile(Encoding.UTF8.GetBytes(""), out _);
            ClassicAssert.IsNull(program);

            program = ExprCompiler.TryCompile(Encoding.UTF8.GetBytes("   "), out _);
            ClassicAssert.IsNull(program);
        }

        [Test]
        public void Compiler_UnexpectedCharacterReturnsFalse()
        {
            var program = ExprCompiler.TryCompile(Encoding.UTF8.GetBytes("@"), out _);
            ClassicAssert.IsNull(program);
        }

        [Test]
        public void Compiler_NullLiteral()
        {
            var program = ExprCompiler.TryCompile(Encoding.UTF8.GetBytes("null"), out _);
            ClassicAssert.IsNotNull(program);
            ClassicAssert.AreEqual(1, program.Length);
            ClassicAssert.AreEqual(ExprTokenType.Null, program.Instructions[0].TokenType);
        }

        [Test]
        public void Compiler_TupleLiteral()
        {
            var program = ExprCompiler.TryCompile(Encoding.UTF8.GetBytes("[1, \"foo\", 42]"), out _);
            ClassicAssert.IsNotNull(program);
            ClassicAssert.AreEqual(1, program.Length);
            ClassicAssert.AreEqual(ExprTokenType.Tuple, program.Instructions[0].TokenType);
            ClassicAssert.AreEqual(3, program.Instructions[0].TupleLength);
        }

        [Test]
        public void Compiler_HyphenInSelector()
        {
            var program = ExprCompiler.TryCompile(Encoding.UTF8.GetBytes(".my-field"), out _);
            ClassicAssert.IsNotNull(program);
            ClassicAssert.AreEqual(1, program.Length);
            ClassicAssert.AreEqual(ExprTokenType.Selector, program.Instructions[0].TokenType);
            ClassicAssert.AreEqual("my-field", program.Instructions[0].Str);
        }

        [Test]
        public void Compiler_PrecedenceMultiplicationBeforeAddition()
        {
            // "1 + 2 * 3" → [1] [2] [3] [*] [+]
            var program = ExprCompiler.TryCompile(Encoding.UTF8.GetBytes("1 + 2 * 3"), out _);
            ClassicAssert.IsNotNull(program);
            ClassicAssert.AreEqual(5, program.Length);
            ClassicAssert.AreEqual(OpCode.Mul, program.Instructions[3].OpCode);
            ClassicAssert.AreEqual(OpCode.Add, program.Instructions[4].OpCode);
        }

        [Test]
        public void Compiler_PrecedenceAndBeforeOr()
        {
            // "true or false and true" → [1] [0] [1] [and] [or]
            var program = ExprCompiler.TryCompile(Encoding.UTF8.GetBytes("true or false and true"), out _);
            ClassicAssert.IsNotNull(program);
            ClassicAssert.AreEqual(5, program.Length);
            ClassicAssert.AreEqual(OpCode.And, program.Instructions[3].OpCode);
            ClassicAssert.AreEqual(OpCode.Or, program.Instructions[4].OpCode);
        }

        [Test]
        public void Compiler_ParenthesesOverridePrecedence()
        {
            // "(1 + 2) * 3" → [1] [2] [+] [3] [*]
            var program = ExprCompiler.TryCompile(Encoding.UTF8.GetBytes("(1 + 2) * 3"), out _);
            ClassicAssert.IsNotNull(program);
            ClassicAssert.AreEqual(5, program.Length);
            ClassicAssert.AreEqual(OpCode.Add, program.Instructions[2].OpCode);
            ClassicAssert.AreEqual(OpCode.Mul, program.Instructions[4].OpCode);
        }

        [Test]
        public void Compiler_ContainmentOperator()
        {
            // '"action" in .tags' → [STR:action] [SEL:tags] [OP:In]
            var program = ExprCompiler.TryCompile(Encoding.UTF8.GetBytes("\"action\" in .tags"), out _);
            ClassicAssert.IsNotNull(program);
            ClassicAssert.AreEqual(3, program.Length);
            ClassicAssert.AreEqual(ExprTokenType.Str, program.Instructions[0].TokenType);
            ClassicAssert.AreEqual(ExprTokenType.Selector, program.Instructions[1].TokenType);
            ClassicAssert.AreEqual(OpCode.In, program.Instructions[2].OpCode);
        }

        [Test]
        public void Compiler_ExponentiationRightAssociative()
        {
            // "2 ** 3 ** 2" → 2 ** (3 ** 2) = 512
            var program = ExprCompiler.TryCompile(Encoding.UTF8.GetBytes("2 ** 3 ** 2"), out _);
            ClassicAssert.IsNotNull(program);
            ClassicAssert.AreEqual(5, program.Length);
            ClassicAssert.AreEqual(OpCode.Pow, program.Instructions[3].OpCode);
            ClassicAssert.AreEqual(OpCode.Pow, program.Instructions[4].OpCode);

            var result = ExprTestHelpers.EvaluateFilter("2 ** 3 ** 2", "{}");
            ClassicAssert.AreEqual(512.0, result.Num);
        }

        [Test]
        public void Compiler_UnaryNot()
        {
            // "not true" → [NUM:1] [OP:Not]
            var program = ExprCompiler.TryCompile(Encoding.UTF8.GetBytes("not true"), out _);
            ClassicAssert.IsNotNull(program);
            ClassicAssert.AreEqual(2, program.Length);
            ClassicAssert.AreEqual(ExprTokenType.Num, program.Instructions[0].TokenType);
            ClassicAssert.AreEqual(ExprTokenType.Op, program.Instructions[1].TokenType);
            ClassicAssert.AreEqual(OpCode.Not, program.Instructions[1].OpCode);
        }

        [Test]
        public void Compiler_ErrorOnMissingClosingParen()
        {
            var program = ExprCompiler.TryCompile(Encoding.UTF8.GetBytes("(1 + 2"), out _);
            ClassicAssert.IsNull(program);
        }

        [Test]
        public void Compiler_ErrorOnUnexpectedToken()
        {
            var program = ExprCompiler.TryCompile(Encoding.UTF8.GetBytes(")"), out _);
            ClassicAssert.IsNull(program);
        }

        [Test]
        public void Compiler_InWithTupleLiteral()
        {
            var program = ExprCompiler.TryCompile(Encoding.UTF8.GetBytes(".director in [\"Spielberg\", \"Nolan\"]"), out _);
            ClassicAssert.IsNotNull(program);
            ClassicAssert.AreEqual(3, program.Length);
            ClassicAssert.AreEqual(ExprTokenType.Selector, program.Instructions[0].TokenType);
            ClassicAssert.AreEqual(ExprTokenType.Tuple, program.Instructions[1].TokenType);
            ClassicAssert.AreEqual(2, program.Instructions[1].TupleLength);
            ClassicAssert.AreEqual(OpCode.In, program.Instructions[2].OpCode);
        }
    }
}
